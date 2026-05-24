#include <Wire.h>
#include <PubSubClient.h>
#include "MAX30100_PulseOximeter.h"
#include <WiFi.h>

#define SDA_PIN 32
#define SCL_PIN 27
#define BUZZER_PIN 25
#define REPORTING_PERIOD_MS 1000
#define MQTT_RETRY_MS 5000

#define LIMITE_BPM 100

const char* ssid = "Michelleb";
const char* password = "280989mi";
const char* mqtt_server = "broker.hivemq.com";
const int mqtt_port = 1883;
const char* mqtt_topic = "healthsensor";
const char* mqtt_beat_topic = "healthsensor/beat";

WiFiClient espClient;
PubSubClient client(espClient);

PulseOximeter pox;
uint32_t tsLastReport = 0;
uint32_t tsLastMqttAttempt = 0;

// Mutex pra proteger acesso ao I2C quando varias tarefas falam com sensor
SemaphoreHandle_t i2cMutex;

void connectToWiFi() {
  Serial.print("Conectando WiFi...");
  WiFi.begin(ssid, password);
  int tries = 0;
  while (WiFi.status() != WL_CONNECTED && tries < 40) {
    delay(250);
    Serial.print(".");
    tries++;
  }
  Serial.println(WiFi.status() == WL_CONNECTED ? " OK" : " Falhou");
  WiFi.setSleep(false);
}

void onBeatDetected() {
  Serial.println("Batimento detectado!");
  if (client.connected()) {
    client.publish(mqtt_beat_topic, "Batimento detectado!");
  }
}

void tryMqttReconnect() {
  if (client.connected()) return;
  if (millis() - tsLastMqttAttempt < MQTT_RETRY_MS) return;
  tsLastMqttAttempt = millis();
  Serial.print("MQTT... ");
  if (client.connect("esp32-client")) {
    Serial.println("OK");
  } else {
    Serial.print("falhou rc=");
    Serial.println(client.state());
  }
}

// >>> TAREFA DEDICADA PRO SENSOR — roda no NUCLEO 0, sem parar
void sensorTask(void* pvParameters) {
  for (;;) {
    if (xSemaphoreTake(i2cMutex, portMAX_DELAY) == pdTRUE) {
      pox.update();
      xSemaphoreGive(i2cMutex);
    }
    vTaskDelay(1 / portTICK_PERIOD_MS);  // ~1ms = 1000Hz
  }
}

void setup() {
  Serial.begin(115200);
  delay(500);

  pinMode(BUZZER_PIN, OUTPUT);
  digitalWrite(BUZZER_PIN, LOW);

  connectToWiFi();

  client.setServer(mqtt_server, mqtt_port);
  client.setCallback([](char* topic, byte* payload, unsigned int length) {});

  Wire.begin(SDA_PIN, SCL_PIN);
  Wire.setClock(100000);

  Serial.print("Oximetro... ");
  if (!pox.begin()) {
    Serial.println("FALHA");
    while (1);
  }
  Serial.println("OK");

  pox.setIRLedCurrent(MAX30100_LED_CURR_24MA);
  pox.setOnBeatDetectedCallback(onBeatDetected);

  i2cMutex = xSemaphoreCreateMutex();

  // Cria tarefa do sensor no NUCLEO 0 (WiFi geralmente usa nucleo 0 tambem,
  // mas com prioridade alta o sensor ganha. Loop() fica no nucleo 1 livre).
  // Se problema continuar, trocar ultimo parametro de 0 para 1.
  xTaskCreatePinnedToCore(
    sensorTask,       // funcao
    "sensorTask",     // nome
    4096,             // stack
    NULL,             // params
    3,                // prioridade ALTA
    NULL,             // handle
    1                 // NUCLEO 1 (loop() tambem, mas com prioridade maior preempta)
  );
}

void loop() {
  // Sensor roda em outra tarefa. Aqui so WiFi/MQTT.
  tryMqttReconnect();
  client.loop();

  if (millis() - tsLastReport > REPORTING_PERIOD_MS) {
    float heartRate, spO2;

    if (xSemaphoreTake(i2cMutex, portMAX_DELAY) == pdTRUE) {
      heartRate = pox.getHeartRate();
      spO2 = pox.getSpO2();
      xSemaphoreGive(i2cMutex);
    }

    Serial.print("HR: ");
    Serial.print(heartRate);
    Serial.print(" bpm | SpO2: ");
    Serial.print(spO2);
    Serial.println(" %");

    if (heartRate > 0 && spO2 > 0) {
      String payload = "{\"heartRate\":";
      payload += heartRate;
      payload += ", \"spO2\":";
      payload += spO2;
      payload += "}";

      if (client.connected()) {
        client.publish(mqtt_topic, payload.c_str());
        Serial.println("MQTT: " + payload);
      }

      if (heartRate > LIMITE_BPM) {
        digitalWrite(BUZZER_PIN, HIGH);
        Serial.println("ALERTA! BPM alto.");
      } else {
        digitalWrite(BUZZER_PIN, LOW);
      }
    }

    tsLastReport = millis();
  }

  delay(10);
}
