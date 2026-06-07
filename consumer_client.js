/**
 * WoT API Scripting Client — Wearable Health Monitor
 * Grupo PG_03 | urn:dev:wot:health-monitor:wearable:patient001
 *
 * Carrega a TD directamente do ficheiro TD_Unified.json
 * e cobre TODAS as properties, actions e events definidos.
 *
 * Dependências:
 *   npm install @node-wot/core @node-wot/binding-http @node-wot/binding-mqtt
 *
 * Uso:
 *   node client.js [caminho/para/TD_Unified.json]
 */

"use strict";

const fs        = require("fs").promises;
const path      = require("path");
const { Servient }        = require("@node-wot/core");
const { HttpClientFactory } = require("@node-wot/binding-http");
const { MqttClientFactory } = require("@node-wot/binding-mqtt");

// ─────────────────────────────────────────────
// Utilitários de log com cores
// ─────────────────────────────────────────────
const C = {
  reset:  "\x1b[0m",
  bold:   "\x1b[1m",
  cyan:   "\x1b[36m",
  green:  "\x1b[32m",
  yellow: "\x1b[33m",
  red:    "\x1b[31m",
  magenta:"\x1b[35m",
  gray:   "\x1b[90m",
};

const log = {
  info:  (m) => console.log(`${C.cyan}[INFO]${C.reset}   ${m}`),
  ok:    (m) => console.log(`${C.green}[OK]${C.reset}     ${m}`),
  warn:  (m) => console.log(`${C.yellow}[WARN]${C.reset}   ${m}`),
  alert: (m) => console.log(`${C.red}[ALERT]${C.reset}  ${m}`),
  data:  (l, v) => console.log(`${C.magenta}[DATA]${C.reset}   ${C.bold}${l}${C.reset} →`, JSON.stringify(v, null, 2)),
  sep:   (t) => console.log(`\n${C.gray}${"─".repeat(50)}${C.reset}\n${C.bold} ${t}${C.reset}`),
};

// ─────────────────────────────────────────────
// 1. PROPERTIES — leitura pontual (readProperty)
// ─────────────────────────────────────────────
async function lerTodasAsProperties(thing) {
  log.sep("Properties → readProperty()");

  // Sensores vitais (só leitura)
  const hr   = await (await thing.readProperty("heartRate")).value();
  const spo2 = await (await thing.readProperty("spO2")).value();
  const temp = await (await thing.readProperty("bodyTemperature")).value();
  const amb  = await (await thing.readProperty("ambientTemperature")).value();
  log.data("heartRate",          hr);
  log.data("spO2",               spo2);
  log.data("bodyTemperature",    temp);
  log.data("ambientTemperature", amb);

  // Estado do atuador
  const vibActive = await (await thing.readProperty("vibrationActive")).value();
  const lastVib   = await (await thing.readProperty("lastVibrationAt")).value();
  log.data("vibrationActive",   vibActive);
  log.data("lastVibrationAt",   lastVib);

  // Estado do dispositivo
  const bat    = await (await thing.readProperty("batteryLevel")).value();
  const conn   = await (await thing.readProperty("connectionStatus")).value();
  log.data("batteryLevel",      bat);
  log.data("connectionStatus",  conn);

  // Configurações (leitura + escrita)
  const interval   = await (await thing.readProperty("samplingIntervalMs")).value();
  const emiss      = await (await thing.readProperty("emissivity")).value();
  const thresholds = await (await thing.readProperty("thresholds")).value();
  log.data("samplingIntervalMs", interval);
  log.data("emissivity",         emiss);
  log.data("thresholds",         thresholds);

  // Dados do paciente associado
  const patient = await (await thing.readProperty("patient")).value();
  log.data("patient", patient);

  return { hr, spo2, temp };
}

// ─────────────────────────────────────────────
// 2. PROPERTIES — escrita (writeProperty)
// ─────────────────────────────────────────────
async function configurarDispositivo(thing) {
  log.sep("Properties → writeProperty()");

  // Alterar intervalo de amostragem para 3s
  await thing.writeProperty("samplingIntervalMs", 3000);
  log.ok("samplingIntervalMs → 3000 ms");

  // Ajustar emissividade (pele mais clara ≈ 0.96)
  await thing.writeProperty("emissivity", 0.96);
  log.ok("emissivity → 0.96");

  // Personalizar limiares clínicos do paciente
  const novosLimiares = {
    heartRate:       { min: 45, max: 160 },
    spO2:            { min: 92 },
    bodyTemperature: { min: 35.5, max: 37.8 },
  };
  await thing.writeProperty("thresholds", novosLimiares);
  log.ok("thresholds actualizados");
  log.data("Novos thresholds", novosLimiares);
}

// ─────────────────────────────────────────────
// 3. PROPERTIES — observação contínua (observeProperty)
//    Cada nova publicação MQTT chega automaticamente
// ─────────────────────────────────────────────
async function observarPropriedades(thing) {
  log.sep("Properties → observeProperty() via MQTT");

  await thing.observeProperty("heartRate", async (interação) => {
    const valor = await interação.value();
    log.data("heartRate [live]", valor);
    if (valor < 45 || valor > 160) {
      log.alert(`Frequência cardíaca fora dos limites: ${valor} bpm`);
    }
  });

  await thing.observeProperty("spO2", async (interação) => {
    const valor = await interação.value();
    log.data("spO2 [live]", valor);
    if (valor < 92) {
      log.alert(`SpO2 crítico: ${valor}% — acionar vibração de emergência!`);
      await acionarVibracaoEmergencia(thing);
    }
  });

  await thing.observeProperty("bodyTemperature", async (interação) => {
    const valor = await interação.value();
    log.data("bodyTemperature [live]", valor);
    if (valor > 37.8) {
      log.warn(`Temperatura elevada: ${valor} °C`);
    }
  });

  await thing.observeProperty("vibrationActive", async (interação) => {
    const ativo = await interação.value();
    log.info(`Motor de vibração: ${ativo ? "ACTIVO" : "inactivo"}`);
  });

  await thing.observeProperty("connectionStatus", async (interação) => {
    const estado = await interação.value();
    log.warn(`Conectividade mudou para: ${estado}`);
  });

  await thing.observeProperty("thresholds", async (interação) => {
    const val = await interação.value();
    log.info("Limiares clínicos actualizados remotamente");
    log.data("thresholds [live]", val);
  });

  log.ok("6 observadores activos (subscriptions MQTT)");
}

// ─────────────────────────────────────────────
// 4. ACTIONS — invocar (invokeAction)
// ─────────────────────────────────────────────
async function acionarVibracaoEmergencia(thing) {
  log.sep("Action → activateVibration (emergência)");
  const input = { duration_ms: 2000, pattern: "sos", intensity: 255 };
  const res   = await thing.invokeAction("activateVibration", input);
  const val   = await res.value();
  log.ok("Vibração SOS activada");
  log.data("Resposta", val); // { success: true, startedAt: "..." }
}

async function acionarVibracaoNormal(thing) {
  log.sep("Action → activateVibration (aviso normal)");
  const input = { duration_ms: 500, pattern: "short", intensity: 180 };
  const res   = await thing.invokeAction("activateVibration", input);
  const val   = await res.value();
  log.ok("Vibração de aviso activada");
  log.data("Resposta", val);
}

async function pararVibracao(thing) {
  log.sep("Action → stopVibration");
  // Idempotente — seguro chamar mesmo que não esteja a vibrar
  await thing.invokeAction("stopVibration");
  log.ok("Vibração parada");
}

async function calibrarSensores(thing, modo = "quick") {
  log.sep(`Action → calibrate (modo: ${modo})`);
  // Assíncrona — pode demorar alguns segundos
  const res = await thing.invokeAction("calibrate", { mode: modo });
  const val = await res.value();
  log.ok("Calibração concluída");
  log.data("Resultado", val); // { success: true, durationMs: 3200 }
}

async function registarNoDirectorio(thing) {
  log.sep("Action → registerDevice");
  const input = { directoryUrl: "https://gateway.wot-health.local:8081/things" };
  await thing.invokeAction("registerDevice", input);
  log.ok("Thing Description publicada no Thing Directory");
}

// ─────────────────────────────────────────────
// 5. EVENTS — subscrever (subscribeEvent)
//    O dispositivo notifica por iniciativa própria
// ─────────────────────────────────────────────
async function subscreverEventos(thing) {
  log.sep("Events → subscribeEvent() via MQTT");

  // Alerta clínico — mais crítico
  await thing.subscribeEvent("criticalHealthAlert", async (interação) => {
    const alerta = await interação.value();
    console.log(`\n${C.red}${"═".repeat(52)}${C.reset}`);
    log.alert(`ALERTA CLÍNICO  |  severidade: ${alerta.severity.toUpperCase()}`);
    log.alert(`Sensor    : ${alerta.source}`);
    log.alert(`Valor     : ${alerta.value}`);
    log.alert(`Limiar    : min=${alerta.threshold?.min ?? "—"}  max=${alerta.threshold?.max ?? "—"}`);
    log.alert(`Paciente  : ${alerta.patientId}`);
    log.alert(`Timestamp : ${alerta.timestamp}`);
    console.log(`${C.red}${"═".repeat(52)}${C.reset}\n`);

    // Reacção automática consoante severidade
    if (alerta.severity === "red") {
      await acionarVibracaoEmergencia(thing);
    } else if (alerta.severity === "yellow") {
      await acionarVibracaoNormal(thing);
    }
  });

  // Notificação de fim de ciclo de vibração
  await thing.subscribeEvent("vibrationCompleted", async (interação) => {
    const ev = await interação.value();
    log.ok(`Vibração concluída — padrão: ${ev.pattern}, duração: ${ev.durationMs}ms`);
    log.data("vibrationCompleted", ev);
  });

  // Mudança de estado do dispositivo / bateria crítica
  await thing.subscribeEvent("deviceStatusChanged", async (interação) => {
    const ev = await interação.value();
    log.warn(`Dispositivo: ${ev.connectionStatus} | bateria: ${ev.batteryLevel}%`);
    if (ev.batteryLevel !== undefined && ev.batteryLevel < 15) {
      log.alert("Bateria crítica (<15%) — notificar cuidador!");
    }
    if (ev.connectionStatus === "offline") {
      log.alert("Dispositivo offline — verificar conectividade!");
    }
  });

  log.ok("3 subscriptions de eventos activas");
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────
async function main() {
  // Caminho da TD: argumento CLI ou ficheiro local
  const tdPath = process.argv[2] ?? path.join(__dirname, "TD_Unified.json");

  console.log(`\n${C.bold} WoT API Scripting Client — Wearable Health Monitor${C.reset}`);
  console.log(` Grupo PG_03 | patient001\n`);

  // Carregar TD do ficheiro
  log.info(`A carregar Thing Description: ${tdPath}`);
  const tdRaw = await fs.readFile(tdPath, "utf-8");
  const TD    = JSON.parse(tdRaw);
  log.ok(`TD carregada: ${TD.id}`);

  // Inicializar Servient com bindings HTTP + MQTT
  const servient = new Servient();
  servient.addClientFactory(new HttpClientFactory());
  servient.addClientFactory(
    new MqttClientFactory({
      uri: "mqtt://localhost:1883",
      // Em produção com TLS-PSK:
      // protocol : "mqtts",
      // psk      : { identity: "esp32-patient001", psk: Buffer.from("PSK_SECRET") },
    })
  );

  const WoT   = await servient.start();

  // Consumir a TD — a partir daqui `thing` tem todos os métodos WoT Scripting API
  const thing = await WoT.consume(TD);
  log.ok("Thing consumida com sucesso\n");

  // ── Executar todas as interacções ──────────
  await lerTodasAsProperties(thing);
  await configurarDispositivo(thing);
  await subscreverEventos(thing);
  await observarPropriedades(thing);
  await calibrarSensores(thing, "quick");
  await registarNoDirectorio(thing);

  // Demonstração do ciclo vibração → parar
  await acionarVibracaoNormal(thing);
  setTimeout(() => pararVibracao(thing), 1500);

  log.sep("Cliente activo — a aguardar dados em tempo real");
  log.info("Ctrl+C para terminar\n");
}

main().catch((err) => {
  console.error(`${C.red}[ERRO]${C.reset}`, err.message);
  process.exit(1);
});
