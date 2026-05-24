/**
 * ============================================================
 *  SERVIENT PRODUCER — Wearable Health Monitor (patient001)
 *  Hardware: Raspberry Pi 4 (gateway WoT / node-wot)
 * ============================================================
 *
 *  Lê dados REAIS do ESP32 via MQTT (broker.hivemq.com):
 *    • Tópico "healthsensor"      → { heartRate, spO2 }  (MAX30100)
 *    • Tópico "healthsensor/beat" → batimento detetado
 *
 *  A propriedade "deviceReady" fica false até o ESP32 publicar
 *  a primeira mensagem MQTT válida. O Consumer deve verificar
 *  este flag antes de iniciar qualquer operação.
 *
 *  Como correr:
 *    npm install @node-wot/core @node-wot/binding-http mqtt
 *    node server_producer.js
 * ============================================================
 */

"use strict";

const { Servient } = require("@node-wot/core");
const { HttpServer } = require("@node-wot/binding-http");
const mqtt = require("mqtt");

const MQTT_BROKER    = "mqtt://broker.hivemq.com:1883";
const TOPIC_SENSOR   = "healthsensor";
const TOPIC_BEAT     = "healthsensor/beat";
const MQTT_CLIENT_ID = `wot-producer-${Math.random().toString(16).slice(2, 8)}`;

const servient = new Servient();
servient.addServer(new HttpServer({ port: 8080 }));

// ─── Estado interno ──────────────────────────────────────────
const state = {
  // Sensores — só têm valor depois do ESP32 ligar
  heartRate:          null,
  spO2:               null,
  bodyTemperature:    null,   // MLX90614 (não publicado pelo firmware atual)
  ambientTemperature: null,

  // Flag de prontidão: false até chegar a 1ª mensagem MQTT real
  deviceReady: false,

  // Configurações graváveis
  samplingIntervalMs: 5000,
  emissivity:         0.98,
  thresholds: {
    heartRate:       { min: 0, max: 100 },
    spO2:            { min: 90             },
    bodyTemperature: { min: 35.0, max: 38.5 }
  },

  // Atuador de vibração
  vibrationActive:  false,
  lastVibrationAt:  null,
  vibrationTimer:   null,

  // Estado do dispositivo
  connectionStatus: "offline",   // começa offline — sem dados reais ainda
  batteryLevel:     null,
  patient: {
    patientId:   "patient001",
    displayName: "Paciente 001",
    age:         65
  },

  // Controlo interno
  _mqttConectado:  false,
  _ultimaMensagem: null
};

let thingRef = null;

// ════════════════════════════════════════════════════════════
//  MECANISMO DE ALERTA BASEADO EM REGRAS
//  Só avalia quando deviceReady = true (dados reais presentes)
// ════════════════════════════════════════════════════════════
function avaliarLimiares() {
  if (!state.deviceReady) return [];

  const t = state.thresholds;
  const alertas = [];

  // Regra 1 — Frequência Cardíaca (igual ao Arduino: alerta se BPM > 100)
  if (state.heartRate !== null && state.heartRate > t.heartRate.max) {
    alertas.push({
      source:    "heartRate",
      value:     state.heartRate,
      threshold: t.heartRate,
      severity:  "red",
      patientId: state.patient.patientId,
      timestamp: new Date().toISOString()
    });
  }

  // Regra 2 — SpO2
  if (state.spO2 !== null && state.spO2 < t.spO2.min) {
    alertas.push({
      source:    "spO2",
      value:     state.spO2,
      threshold: t.spO2,
      severity:  state.spO2 < t.spO2.min - 5 ? "red" : "yellow",
      patientId: state.patient.patientId,
      timestamp: new Date().toISOString()
    });
  }

  // Regra 3 — Temperatura Corporal (quando disponível)
  if (state.bodyTemperature !== null) {
    const { min: tMin, max: tMax } = t.bodyTemperature;
    if (state.bodyTemperature < tMin || state.bodyTemperature > tMax) {
      alertas.push({
        source:    "bodyTemperature",
        value:     state.bodyTemperature,
        threshold: t.bodyTemperature,
        severity:  state.bodyTemperature > tMax + 1.0 ? "red" : "yellow",
        patientId: state.patient.patientId,
        timestamp: new Date().toISOString()
      });
    }
  }

  return alertas;
}

// ════════════════════════════════════════════════════════════
//  CLIENTE MQTT — recebe dados reais do ESP32
// ════════════════════════════════════════════════════════════
function iniciarMQTT() {
  console.log(`🔌 A ligar ao broker MQTT: ${MQTT_BROKER} ...`);

  const mqttClient = mqtt.connect(MQTT_BROKER, {
    clientId:        MQTT_CLIENT_ID,
    clean:           true,
    reconnectPeriod: 5000,
    connectTimeout:  10000
  });

  mqttClient.on("connect", () => {
    state._mqttConectado = true;
    console.log("✅ MQTT ligado ao broker.hivemq.com");
    console.log(`📡 À espera de dados do ESP32 nos tópicos: "${TOPIC_SENSOR}", "${TOPIC_BEAT}"`);
    mqttClient.subscribe([TOPIC_SENSOR, TOPIC_BEAT], { qos: 1 });
  });

  mqttClient.on("message", (topic, payload) => {
    const mensagem = payload.toString();
    state._ultimaMensagem = new Date().toISOString();

    // ── Batimento detetado pelo callback do MAX30100 ─────
    if (topic === TOPIC_BEAT) {
      console.log(`[MQTT] 💗 ${mensagem}`);
      return;
    }

    // ── Dados dos sensores ───────────────────────────────
    if (topic === TOPIC_SENSOR) {
      let dados;
      try {
        dados = JSON.parse(mensagem);
      } catch {
        console.warn(`[MQTT] ⚠️  Payload inválido: ${mensagem}`);
        return;
      }

      const hr   = parseFloat(dados.heartRate);
      const spo2 = parseFloat(dados.spO2);

      // Só aceita valores fisiologicamente plausíveis
      // HR < 30 ou SpO2 < 50 = sensor sem dedo ou leitura inválida
      if (isNaN(hr) || hr < 30 || hr > 220 || isNaN(spo2) || spo2 < 50 || spo2 > 100) {
        console.warn(`[MQTT] ⚠️  Leitura ignorada (sensor sem dedo?): HR=${dados.heartRate} SpO2=${dados.spO2}`);
        return;
      }

      state.heartRate = Math.round(hr);
      state.spO2      = parseFloat(spo2.toFixed(1));

      // Primeira leitura real: o dispositivo fica pronto
      if (!state.deviceReady) {
        state.deviceReady      = true;
        state.connectionStatus = "online";
        console.log("\n✅ ESP32 ligado — primeira leitura real recebida. deviceReady = true\n");

        if (thingRef) {
          thingRef.emitEvent("deviceStatusChanged", {
            connectionStatus: "online",
            batteryLevel:     state.batteryLevel,
            mqttConectado:    true,
            timestamp:        new Date().toISOString()
          });
        }
      }

      console.log(
        `[MQTT] 📥 ESP32 → BPM: ${state.heartRate}` +
        ` | SpO2: ${state.spO2}%`
      );

      // Avalia limiares com dados reais
      if (thingRef && state.deviceReady) {
        const alertas = avaliarLimiares();
        for (const alerta of alertas) {
          const label = alerta.severity === "red" ? "🔴 CRÍTICO" : "🟡 ATENÇÃO";
          console.log(`🚨 ALERTA ${label} | ${alerta.source} = ${alerta.value}`);
          thingRef.emitEvent("criticalHealthAlert", alerta);

          // Vibração SOS automática em alertas críticos
          if (alerta.severity === "red" && !state.vibrationActive) {
            console.log("📳 Auto-vibração SOS (alerta crítico)");
            state.vibrationActive = true;
            state.lastVibrationAt = new Date().toISOString();
            if (state.vibrationTimer) clearTimeout(state.vibrationTimer);
            state.vibrationTimer = setTimeout(() => {
              state.vibrationActive = false;
              thingRef.emitEvent("vibrationCompleted", {
                durationMs: 3000, pattern: "sos",
                timestamp: new Date().toISOString()
              });
            }, 3000);
          }
        }
      }
    }
  });

  mqttClient.on("reconnect", () => {
    state._mqttConectado   = false;
    state.connectionStatus = "reconnecting";
    console.warn("🔄 MQTT a reconectar...");
  });

  mqttClient.on("offline", () => {
    state._mqttConectado   = false;
    state.connectionStatus = "offline";
    state.deviceReady      = false;
    console.warn("⚠️  MQTT offline — deviceReady = false");
    if (thingRef) {
      thingRef.emitEvent("deviceStatusChanged", {
        connectionStatus: "offline",
        batteryLevel:     state.batteryLevel,
        mqttConectado:    false,
        timestamp:        new Date().toISOString()
      });
    }
  });

  mqttClient.on("error", (err) => {
    console.error("❌ Erro MQTT:", err.message);
  });

  return mqttClient;
}

// ════════════════════════════════════════════════════════════
//  ARRANQUE DO SERVIENT PRODUCER
// ════════════════════════════════════════════════════════════
servient.start().then(async (WoT) => {

  const thing = await WoT.produce({
    "@context": [
      "https://www.w3.org/2022/wot/td/v1.1",
      {
        "healthiot": "https://w3id.org/iotschema/health#",
        "saref":     "https://saref.etsi.org/core/",
        "om":        "http://www.ontology-of-units-of-measure.org/resource/om-2/"
      }
    ],
    "@type":     ["Thing", "healthiot:WearableHealthMonitor", "saref:Actuator"],
    id:          "urn:dev:wot:health-monitor:wearable:patient001",
    title:       "patient001",
    description: "Dispositivo vestível ESP32 — MAX30100 (BPM+SpO2), MLX90614 (temperatura), atuador de vibração. Dados reais via MQTT.",
    version:     { instance: "1.0.0" },
    securityDefinitions: { nosec_sc: { scheme: "nosec" } },
    security:    ["nosec_sc"],

    properties: {
      // ── Prontidão do dispositivo ─────────────────────────
      deviceReady: {
        title:       "Dispositivo pronto",
        description: "true apenas quando o ESP32 está ligado e a publicar dados reais via MQTT.",
        type:        "boolean",
        readOnly:    true,
        observable:  true
      },
      // ── Sensores ─────────────────────────────────────────
      heartRate: {
        "@type": "healthiot:HeartRate",
        title: "Frequência Cardíaca",
        description: "BPM reais do ESP32/MAX30100 via MQTT. 0 enquanto não há leitura.",
        type: "integer",
        unit: "om:beats-per-minute",
        minimum: 0, maximum: 220,
        readOnly: true, observable: true
      },
      spO2: {
        "@type": "healthiot:OxygenSaturation",
        title: "Saturação de Oxigénio (SpO2)",
        type: "number",
        unit: "om:percent",
        minimum: 0, maximum: 100,
        readOnly: true, observable: true
      },
      bodyTemperature: {
        "@type": "healthiot:BodyTemperature",
        title: "Temperatura Corporal",
        type: "number",
        unit: "om:degree-Celsius",
        minimum: 0, maximum: 45,
        readOnly: true, observable: true
      },
      ambientTemperature: {
        title: "Temperatura Ambiente",
        type: "number",
        unit: "om:degree-Celsius",
        minimum: 0, maximum: 60,
        readOnly: true, observable: false
      },
      // ── Configurações graváveis ───────────────────────────
      samplingIntervalMs: {
        title: "Intervalo de amostragem (ms)",
        type: "integer", unit: "om:millisecond",
        minimum: 1000, maximum: 60000, default: 5000,
        readOnly: false
      },
      emissivity: {
        title: "Emissividade (MLX90614)",
        type: "number", minimum: 0.1, maximum: 1.0, default: 0.98,
        readOnly: false
      },
      thresholds: {
        title: "Limiares Clínicos Personalizados",
        type: "object", readOnly: false, observable: true,
        properties: {
          heartRate:       { type: "object" },
          spO2:            { type: "object" },
          bodyTemperature: { type: "object" }
        }
      },
      // ── Atuador ───────────────────────────────────────────
      vibrationActive: {
        title: "Vibração activa",
        type: "boolean", readOnly: true, observable: true
      },
      lastVibrationAt: {
        title: "Última activação do atuador",
        type: "string", readOnly: true
      },
      // ── Estado do dispositivo ─────────────────────────────
      connectionStatus: {
        title: "Estado de Conectividade",
        type: "string", enum: ["online", "offline", "reconnecting"],
        readOnly: true, observable: true
      },
      batteryLevel: {
        title: "Nível de Bateria (%)",
        type: "integer",
        unit: "om:percent", minimum: 0, maximum: 100,
        readOnly: true, observable: true
      },
      patient: {
        title: "Paciente associado",
        type: "object", readOnly: true
      }
    },

    actions: {
      activateVibration: {
        "@type": "healthiot:HapticAlert",
        title: "Activar vibração",
        synchronous: true,
        input: {
          type: "object",
          required: ["duration_ms", "pattern"],
          properties: {
            duration_ms: { type: "integer", minimum: 100, maximum: 10000 },
            pattern:     { type: "string",  enum: ["short", "long", "sos"], default: "short" },
            intensity:   { type: "integer", minimum: 0, maximum: 255, default: 200 }
          }
        },
        output: {
          type: "object",
          properties: {
            success:   { type: "boolean" },
            startedAt: { type: "string", format: "date-time" }
          }
        }
      },
      stopVibration: {
        title: "Parar vibração", idempotent: true, safe: true
      },
      calibrate: {
        title: "Calibrar sensores", synchronous: false,
        input:  { type: "object", properties: { mode: { type: "string", enum: ["quick", "full"], default: "quick" } } },
        output: { type: "object", properties: { success: { type: "boolean" }, durationMs: { type: "integer" } } }
      },
      registerDevice: {
        title: "Registar no Thing Directory", idempotent: true,
        input: { type: "object", properties: { directoryUrl: { type: "string", format: "uri" } } }
      }
    },

    events: {
      criticalHealthAlert: {
        "@type": "healthiot:CriticalHealthAlert",
        title: "Alerta Clínico Crítico",
        description: "Emitido apenas quando valores reais do sensor violam limiares clínicos.",
        data: {
          type: "object",
          required: ["source", "value", "severity", "timestamp"],
          properties: {
            source:    { type: "string", enum: ["heartRate", "spO2", "bodyTemperature"] },
            value:     { type: "number" },
            threshold: { type: "object" },
            severity:  { type: "string", enum: ["green", "yellow", "red"] },
            patientId: { type: "string" },
            timestamp: { type: "string", format: "date-time" }
          }
        }
      },
      vibrationCompleted: {
        title: "Vibração concluída",
        data: {
          type: "object",
          properties: {
            durationMs: { type: "integer" },
            pattern:    { type: "string" },
            timestamp:  { type: "string", format: "date-time" }
          }
        }
      },
      deviceStatusChanged: {
        title: "Mudança de estado do dispositivo",
        data: {
          type: "object",
          properties: {
            connectionStatus: { type: "string" },
            batteryLevel:     { type: "integer" },
            mqttConectado:    { type: "boolean" },
            timestamp:        { type: "string", format: "date-time" }
          }
        }
      }
    }
  });

  thingRef = thing;

  // ── Handlers de leitura ──────────────────────────────────
  // Sensores numéricos: devolvem 0 enquanto não há dados reais (state = null)
  thing.setPropertyReadHandler("heartRate",          async () => state.heartRate          ?? 0);
  thing.setPropertyReadHandler("spO2",               async () => state.spO2               ?? 0);
  thing.setPropertyReadHandler("bodyTemperature",    async () => state.bodyTemperature    ?? 0);
  thing.setPropertyReadHandler("ambientTemperature", async () => state.ambientTemperature ?? 0);
  thing.setPropertyReadHandler("batteryLevel",       async () => state.batteryLevel       ?? 0);
  for (const key of [
    "deviceReady", "vibrationActive", "lastVibrationAt",
    "connectionStatus", "patient"
  ]) {
    thing.setPropertyReadHandler(key, async () => state[key]);
  }
  thing.setPropertyReadHandler("samplingIntervalMs", async () => state.samplingIntervalMs);
  thing.setPropertyReadHandler("emissivity",         async () => state.emissivity);
  thing.setPropertyReadHandler("thresholds",         async () => state.thresholds);

  // ── Handlers de escrita ──────────────────────────────────

  thing.setPropertyWriteHandler("samplingIntervalMs", async (val) => {
    const novo = await val.value();
    if (typeof novo !== "number" || novo < 1000 || novo > 60000)
      throw new Error(`samplingIntervalMs fora do intervalo [1000, 60000]: ${novo}`);
    state.samplingIntervalMs = novo;
    console.log(`⚙️  samplingIntervalMs → ${novo} ms`);
  });

  thing.setPropertyWriteHandler("emissivity", async (val) => {
    const novo = await val.value();
    if (typeof novo !== "number" || novo < 0.1 || novo > 1.0)
      throw new Error(`emissivity fora do intervalo [0.1, 1.0]: ${novo}`);
    state.emissivity = novo;
    console.log(`⚙️  emissivity → ${novo}`);
  });

  thing.setPropertyWriteHandler("thresholds", async (val) => {
    const novo = await val.value();
    if (novo.heartRate)
      state.thresholds.heartRate = { ...state.thresholds.heartRate, ...novo.heartRate };
    if (novo.spO2)
      state.thresholds.spO2 = { ...state.thresholds.spO2, ...novo.spO2 };
    if (novo.bodyTemperature)
      state.thresholds.bodyTemperature = { ...state.thresholds.bodyTemperature, ...novo.bodyTemperature };
    console.log("⚙️  thresholds →", JSON.stringify(state.thresholds));
  });

  // ── Handlers de ações ────────────────────────────────────

  thing.setActionHandler("activateVibration", async (params) => {
    const { duration_ms, pattern = "short", intensity = 200 } = await params.value();
    if (!duration_ms || duration_ms < 100 || duration_ms > 10000)
      throw new Error(`duration_ms inválido: ${duration_ms}`);
    if (!["short", "long", "sos"].includes(pattern))
      throw new Error(`pattern inválido: '${pattern}'`);

    if (state.vibrationActive && state.vibrationTimer)
      clearTimeout(state.vibrationTimer);

    state.vibrationActive = true;
    state.lastVibrationAt = new Date().toISOString();
    console.log(`📳 activateVibration | pattern=${pattern} | ${duration_ms}ms | intensity=${intensity}`);

    state.vibrationTimer = setTimeout(() => {
      state.vibrationActive = false;
      thingRef.emitEvent("vibrationCompleted", {
        durationMs: duration_ms, pattern,
        timestamp: new Date().toISOString()
      });
      console.log(`📳 vibração concluída (${pattern}, ${duration_ms}ms)`);
    }, duration_ms);

    return { success: true, startedAt: state.lastVibrationAt };
  });

  thing.setActionHandler("stopVibration", async () => {
    if (state.vibrationTimer) clearTimeout(state.vibrationTimer);
    state.vibrationActive = false;
    console.log("🛑 stopVibration");
    return { success: true };
  });

  thing.setActionHandler("calibrate", async (params) => {
    const input   = params ? await params.value() : {};
    const modo    = input?.mode ?? "quick";
    const duracao = modo === "full" ? 5000 : 1200;
    console.log(`🛠  calibrate [${modo}] → ${duracao}ms`);
    return { success: true, durationMs: duracao };
  });

  thing.setActionHandler("registerDevice", async (params) => {
    const input = params ? await params.value() : {};
    const url   = input?.directoryUrl ?? "https://gateway.wot-health.local:8081/things";
    console.log(`📒 registerDevice → ${url}`);
    return { success: true, registeredAt: new Date().toISOString() };
  });

  // ── Inicia ligação MQTT ───────────────────────────────────
  iniciarMQTT();

  // ── deviceStatusChanged periódico (bateria + estado) ─────
  setInterval(() => {
    thing.emitEvent("deviceStatusChanged", {
      connectionStatus: state.connectionStatus,
      batteryLevel:     state.batteryLevel,
      mqttConectado:    state._mqttConectado,
      timestamp:        new Date().toISOString()
    });
  }, 10000);

  await thing.expose();

  console.log("\n🚀 Servient Producer ativo");
  console.log("   Thing Description → GET http://localhost:8080/patient001");
  console.log("   À espera de dados do ESP32 via MQTT...");
  console.log("   (deviceReady = false até chegar a 1ª mensagem real)\n");

}).catch((err) => {
  console.error("❌ Erro ao iniciar o Servient Producer:", err);
  process.exit(1);
});
