/**
 * WoT API Scripting Client — Wearable Health Monitor
 * Grupo PG_03 | urn:dev:wot:health-monitor:wearable:patient001
 *
 * Obtém a TD directamente do Producer via HTTP.
 *
 * Uso:
 *   node consumer_client.js                    ← TD do Producer (recomendado)
 *   node consumer_client.js TD_Unified.json    ← TD do ficheiro (fallback)
 */

"use strict";

const fs   = require("fs").promises;
const path = require("path");
const { Servient, Helpers }   = require("@node-wot/core");
const { HttpClientFactory }   = require("@node-wot/binding-http");
const { MqttClientFactory }   = require("@node-wot/binding-mqtt");

const PRODUCER_URL = "http://localhost:8080/patient001";
const MQTT_BROKER  = "mqtt://test.mosquitto.org:1883";

// ── FIX 2: guard contra vibração em cascata ──────────────────────────────────
// Impede que o Consumer invoque nova vibração enquanto uma já está em curso
let _vibracaoEmCurso = false;
const GUARD_VIBRACAO_MS = 2500;  // mínimo entre vibrações consecutivas no Consumer

const C = {
  reset:"\x1b[0m", bold:"\x1b[1m", cyan:"\x1b[36m", green:"\x1b[32m",
  yellow:"\x1b[33m", red:"\x1b[31m", magenta:"\x1b[35m", gray:"\x1b[90m",
};
const log = {
  info:  (m) => console.log(`${C.cyan}[INFO]${C.reset}   ${m}`),
  ok:    (m) => console.log(`${C.green}[OK]${C.reset}     ${m}`),
  warn:  (m) => console.log(`${C.yellow}[WARN]${C.reset}   ${m}`),
  alert: (m) => console.log(`${C.red}[ALERT]${C.reset}  ${m}`),
  data:  (l, v) => console.log(`${C.magenta}[DATA]${C.reset}   ${C.bold}${l}${C.reset} →`, JSON.stringify(v, null, 2)),
  sep:   (t) => console.log(`\n${C.gray}${"─".repeat(50)}${C.reset}\n${C.bold} ${t}${C.reset}`),
};

// ─── Obtém a TD (do Producer via HTTP ou do ficheiro local) ──────────────────
async function obterTD(servient, arg) {
  if (!arg || arg.startsWith("http")) {
    const url = arg ?? PRODUCER_URL;
    log.info(`A obter TD do Producer: ${url}`);
    try {
      const td = await new Helpers(servient).fetch(url);
      log.ok(`TD obtida via HTTP: ${td.id}`);
      return td;
    } catch (err) {
      throw new Error(
        `Não foi possível obter a TD de ${url}.\n` +
        `   Confirma que o Producer está a correr: node producer_server.js\n` +
        `   Detalhe: ${err.message}`
      );
    }
  }
  log.warn(`A carregar TD do ficheiro: ${arg}`);
  const td = JSON.parse(await fs.readFile(arg, "utf-8"));
  log.ok(`TD carregada do ficheiro: ${td.id}`);
  return td;
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. PROPERTIES — readProperty
// ─────────────────────────────────────────────────────────────────────────────
async function lerTodasAsProperties(thing) {
  log.sep("Properties → readProperty()");

  const hr   = await (await thing.readProperty("heartRate")).value();
  const spo2 = await (await thing.readProperty("spO2")).value();
  log.data("heartRate", hr);
  log.data("spO2",      spo2);
  // bodyTemperature e ambientTemperature omitidos — hardware não tem sensor de temperatura

  const fallActive    = await (await thing.readProperty("fallDetectionActive")).value();
  const lastFallEvent = await (await thing.readProperty("lastFallEvent")).value();
  log.data("fallDetectionActive", fallActive);
  log.data("lastFallEvent",       lastFallEvent);

  const vibActive = await (await thing.readProperty("vibrationActive")).value();
  const lastVib   = await (await thing.readProperty("lastVibrationAt")).value();
  log.data("vibrationActive",  vibActive);
  log.data("lastVibrationAt",  lastVib);

  // batteryLevel não tem sensor real — Producer devolve 0 como placeholder
  const bat  = await (await thing.readProperty("batteryLevel")).value();
  const conn = await (await thing.readProperty("connectionStatus")).value();
  log.data("batteryLevel (placeholder)", bat);
  log.data("connectionStatus",           conn);

  const interval   = await (await thing.readProperty("samplingIntervalMs")).value();
  const thresholds = await (await thing.readProperty("thresholds")).value();
  log.data("samplingIntervalMs", interval);
  log.data("thresholds",         thresholds);

  const patient = await (await thing.readProperty("patient")).value();
  log.data("patient", patient);

  return { hr, spo2, fallActive };
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. PROPERTIES — writeProperty
// ─────────────────────────────────────────────────────────────────────────────
async function configurarDispositivo(thing) {
  log.sep("Properties → writeProperty()");
  await thing.writeProperty("samplingIntervalMs", 3000);
  log.ok("samplingIntervalMs → 3000 ms");
  // emissivity removido — sem sensor MLX90614 no hardware
  const novosLimiares = {
    heartRate: { min: 45, max: 160 }, spO2: { min: 92 },
  };
  await thing.writeProperty("thresholds", novosLimiares);
  log.ok("thresholds actualizados");
  log.data("Novos thresholds", novosLimiares);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. PROPERTIES — observeProperty via MQTT
// ─────────────────────────────────────────────────────────────────────────────
async function observarPropriedades(thing) {
  log.sep("Properties → observeProperty() via MQTT");
  await thing.observeProperty("heartRate", async (i) => {
    const v = await i.value();
    log.data("heartRate [live]", v);
    if (v < 45 || v > 160) log.alert(`Frequência cardíaca fora dos limites: ${v} bpm`);
  });
  await thing.observeProperty("spO2", async (i) => {
    const v = await i.value();
    log.data("spO2 [live]", v);
    if (v < 92) {
      log.alert(`SpO2 crítico: ${v}%`);
      await acionarVibracaoEmergencia(thing);
    }
  });
  // bodyTemperature removido — sem sensor de temperatura
  await thing.observeProperty("fallDetectionActive", async (i) => {
    const v = await i.value();
    if (v) log.alert("Queda/impacto activo!");
    else   log.info("Estado de queda limpo");
  });
  await thing.observeProperty("vibrationActive", async (i) => {
    const v = await i.value();
    log.info(`Motor de vibração: ${v ? "ACTIVO" : "inactivo"}`);
  });
  await thing.observeProperty("connectionStatus", async (i) => {
    const v = await i.value();
    log.warn(`Conectividade: ${v}`);
  });
  await thing.observeProperty("thresholds", async (i) => {
    const v = await i.value();
    log.info("Limiares actualizados remotamente");
    log.data("thresholds [live]", v);
  });
  log.ok("7 observadores activos");
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. ACTIONS — invokeAction
//    FIX 2: todas as funções de vibração verificam _vibracaoEmCurso
// ─────────────────────────────────────────────────────────────────────────────
async function acionarVibracaoEmergencia(thing) {
  if (_vibracaoEmCurso) {
    log.warn("Vibração já em curso — SOS ignorado");
    return;
  }
  _vibracaoEmCurso = true;
  setTimeout(() => { _vibracaoEmCurso = false; }, GUARD_VIBRACAO_MS);
  log.sep("Action → activateVibration (emergência SOS)");
  const res = await thing.invokeAction("activateVibration",
    { duration_ms: 2000, pattern: "sos", intensity: 255 });
  log.ok("Vibração SOS activada");
  log.data("Resposta", await res.value());
}

async function acionarVibracaoNormal(thing) {
  if (_vibracaoEmCurso) {
    log.warn("Vibração já em curso — aviso ignorado");
    return;
  }
  _vibracaoEmCurso = true;
  setTimeout(() => { _vibracaoEmCurso = false; }, GUARD_VIBRACAO_MS);
  log.sep("Action → activateVibration (aviso normal)");
  const res = await thing.invokeAction("activateVibration",
    { duration_ms: 500, pattern: "short", intensity: 180 });
  log.ok("Vibração de aviso activada");
  log.data("Resposta", await res.value());
}

async function pararVibracao(thing) {
  log.sep("Action → stopVibration");
  await thing.invokeAction("stopVibration");
  _vibracaoEmCurso = false;
  log.ok("Vibração parada");
}

async function calibrarSensores(thing, modo = "quick") {
  log.sep(`Action → calibrate (modo: ${modo})`);
  const res = await thing.invokeAction("calibrate", { mode: modo });
  log.ok("Calibração concluída");
  log.data("Resultado", await res.value());
}

async function registarNoDirectorio(thing) {
  log.sep("Action → registerDevice");
  await thing.invokeAction("registerDevice",
    { directoryUrl: "https://gateway.wot-health.local:8081/things" });
  log.ok("Thing Description publicada no Thing Directory");
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. EVENTS — subscribeEvent
// ─────────────────────────────────────────────────────────────────────────────
async function subscreverEventos(thing) {
  log.sep("Events → subscribeEvent()");

  // Alerta clínico
  await thing.subscribeEvent("criticalHealthAlert", async (i) => {
    const a = await i.value();
    console.log(`\n${C.red}${"═".repeat(52)}${C.reset}`);
    log.alert(`ALERTA CLÍNICO | severidade: ${a.severity.toUpperCase()}`);
    log.alert(`Sensor: ${a.source} | Valor: ${a.value}`);
    log.alert(`Limiar: min=${a.threshold?.min ?? "—"} max=${a.threshold?.max ?? "—"}`);
    log.alert(`Paciente: ${a.patientId} | ${a.timestamp}`);
    console.log(`${C.red}${"═".repeat(52)}${C.reset}\n`);
    if (a.severity === "red")         await acionarVibracaoEmergencia(thing);
    else if (a.severity === "yellow") await acionarVibracaoNormal(thing);
  });

  // Queda / Impacto — FIX 2: guard já está nas funções de vibração
  await thing.subscribeEvent("fallDetected", async (i) => {
    const ev = await i.value();
    console.log(`\n${C.red}${"█".repeat(52)}${C.reset}`);
    if (ev.tipo === "queda") {
      log.alert("🚨 QUEDA DETECTADA — EMERGÊNCIA!");
      log.alert(`   Intensidade: ${ev.intensidade} | Contagem: ${ev.contagem} impulsos/500ms`);
      log.alert(`   Paciente: ${ev.patientId} | ${ev.timestamp}`);
      console.log(`${C.red}${"█".repeat(52)}${C.reset}\n`);
      await acionarVibracaoEmergencia(thing);
    } else {
      log.warn(`⚠️  Impacto forte | contagem=${ev.contagem} | ${ev.intensidade}`);
      log.warn(`   Paciente: ${ev.patientId} | ${ev.timestamp}`);
      console.log(`${C.red}${"█".repeat(52)}${C.reset}\n`);
      await acionarVibracaoNormal(thing);
    }
  });

  // Vibração concluída
  await thing.subscribeEvent("vibrationCompleted", async (i) => {
    const ev = await i.value();
    log.ok(`Vibração concluída — padrão: ${ev.pattern}, duração: ${ev.durationMs}ms`);
    log.data("vibrationCompleted", ev);
  });

  // Estado do dispositivo
  // batteryLevel não é medido pelo hardware — não incluído no payload do evento
  await thing.subscribeEvent("deviceStatusChanged", async (i) => {
    const ev = await i.value();
    log.warn(`Dispositivo: ${ev.connectionStatus} | MQTT: ${ev.mqttConectado}`);
    if (ev.connectionStatus === "offline") log.alert("Dispositivo offline!");
  });

  log.ok("4 subscriptions activas (criticalHealthAlert, fallDetected, vibrationCompleted, deviceStatusChanged)");
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`\n${C.bold} WoT API Scripting Client — Wearable Health Monitor${C.reset}`);
  console.log(` Grupo PG_03 | patient001 | Broker: test.mosquitto.org\n`);

  const servient = new Servient();
  servient.addClientFactory(new HttpClientFactory());
  servient.addClientFactory(new MqttClientFactory({ uri: MQTT_BROKER }));
  const WoT = await servient.start();

  const TD    = await obterTD(servient, process.argv[2]);
  const thing = await WoT.consume(TD);
  log.ok("Thing consumida com sucesso\n");

  await lerTodasAsProperties(thing);
  await configurarDispositivo(thing);
  await subscreverEventos(thing);
  await observarPropriedades(thing);
  await calibrarSensores(thing, "quick");
  await registarNoDirectorio(thing);

  await acionarVibracaoNormal(thing);
  setTimeout(() => pararVibracao(thing), 1500);

  log.sep("Cliente activo — a aguardar dados em tempo real");
  log.info("Ctrl+C para terminar\n");
}

main().catch((err) => {
  console.error(`\n${C.red}[ERRO]${C.reset} ${err.message}\n`);
  process.exit(1);
});
