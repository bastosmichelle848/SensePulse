/**
 * ============================================================
 *  SERVIENT PRODUCER — Wearable Health Monitor (patient001)
 *  Hardware: Raspberry Pi 4 (gateway WoT / node-wot)
 * ============================================================
 *
 *  Expõe via HTTP as propriedades, ações e eventos do
 *  dispositivo vestível ESP32 (MAX30100 + MLX90614 + vibração).
 *
 *  Como correr:
 *    npm install @node-wot/core @node-wot/binding-http
 *    node server.js
 *
 *  Opcional (MQTT):
 *    npm install @node-wot/binding-mqtt
 *    (requer Mosquitto em mqtt://localhost:1883)
 * ============================================================
 */

"use strict";

const { Servient } = require("@node-wot/core");
const { HttpServer } = require("@node-wot/binding-http");
// const { MqttBrokerServer } = require("@node-wot/binding-mqtt");

// ─── Configuração dos servidores ────────────────────────────
const servient = new Servient();
servient.addServer(new HttpServer({ port: 8080 }));

// Descomenta para activar MQTT:
// servient.addServer(new MqttBrokerServer({ uri: "mqtt://localhost:1883" }));

// ─── Estado interno simulado do dispositivo ─────────────────
const state = {
  // Sensores
  heartRate: 72,
  spO2: 98.0,
  bodyTemperature: 36.5,
  ambientTemperature: 22.0,

  // Configurações graváveis
  samplingIntervalMs: 5000,
  emissivity: 0.98,
  thresholds: {
    heartRate:       { min: 50,  max: 150 },
    spO2:            { min: 90 },
    bodyTemperature: { min: 35.0, max: 38.5 }
  },

  // Atuador de vibração
  vibrationActive: false,
  lastVibrationAt: null,
  vibrationTimer: null,

  // Estado do dispositivo
  connectionStatus: "online",
  batteryLevel: 87,
  patient: {
    patientId:   "patient001",
    displayName: "Paciente 001",
    age:         65
  }
};

// ─── Mecanismo de alerta baseado em regras ──────────────────
/**
 * Avalia os limiares clínicos definidos em state.thresholds.
 * Retorna um objecto de alerta se houver violação, ou null.
 * Severidade: "yellow" → ligeira; "red" → crítica.
 */
function avaliarLimiares(thing) {
  const t = state.thresholds;
  const alertas = [];

  // Regra 1 — Frequência cardíaca
  const { min: hrMin, max: hrMax } = t.heartRate;
  if (state.heartRate < hrMin || state.heartRate > hrMax) {
    const desvio = Math.max(
      hrMin - state.heartRate,
      state.heartRate - hrMax
    );
    alertas.push({
      source:    "heartRate",
      value:     state.heartRate,
      threshold: t.heartRate,
      severity:  desvio > 30 ? "red" : "yellow",
      patientId: state.patient.patientId,
      timestamp: new Date().toISOString()
    });
  }

  // Regra 2 — SpO2
  if (state.spO2 < t.spO2.min) {
    alertas.push({
      source:    "spO2",
      value:     state.spO2,
      threshold: t.spO2,
      severity:  state.spO2 < t.spO2.min - 5 ? "red" : "yellow",
      patientId: state.patient.patientId,
      timestamp: new Date().toISOString()
    });
  }

  // Regra 3 — Temperatura corporal
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

  return alertas;
}

// ────────────────────────────────────────────────────────────
servient.start().then(async (WoT) => {

  // ── Definição inline da Thing (node-wot gera os forms reais) ──
  const thing = await WoT.produce({
    "@context": [
      "https://www.w3.org/2022/wot/td/v1.1",
      {
        "healthiot": "https://w3id.org/iotschema/health#",
        "saref":     "https://saref.etsi.org/core/",
        "om":        "http://www.ontology-of-units-of-measure.org/resource/om-2/"
      }
    ],
    "@type": ["Thing", "healthiot:WearableHealthMonitor"],
    id:          "urn:dev:wot:health-monitor:wearable:patient001",
    title:       "patient001",
    description: "Dispositivo vestível ESP32 — MAX30100 (BPM+SpO2), MLX90614 (temperatura), atuador de vibração háptico. Exposto pelo gateway Raspberry Pi via node-wot.",
    version:     { instance: "1.0.0" },
    securityDefinitions: { nosec_sc: { scheme: "nosec" } },
    security: ["nosec_sc"],

    // ── Propriedades ───────────────────────────────────────────
    properties: {
      // --- Sensores (só leitura) ---
      heartRate: {
        "@type":   "healthiot:HeartRate",
        title:     "Frequência Cardíaca",
        description: "BPM medidos pelo MAX30100 via PPG.",
        type:      "integer",
        unit:      "om:beats-per-minute",
        minimum:   30, maximum: 220,
        readOnly:  true, observable: true
      },
      spO2: {
        "@type":   "healthiot:OxygenSaturation",
        title:     "Saturação de Oxigénio (SpO2)",
        description: "% de hemoglobina oxigenada (MAX30100).",
        type:      "number",
        unit:      "om:percent",
        minimum:   0, maximum: 100,
        readOnly:  true, observable: true
      },
      bodyTemperature: {
        "@type":   "healthiot:BodyTemperature",
        title:     "Temperatura Corporal",
        description: "Temperatura da pele em °C (MLX90614, IR, ±0.5 °C).",
        type:      "number",
        unit:      "om:degree-Celsius",
        minimum:   20.0, maximum: 45.0,
        readOnly:  true, observable: true
      },
      ambientTemperature: {
        title:     "Temperatura Ambiente",
        description: "Temperatura do ambiente para compensação térmica (MLX90614).",
        type:      "number",
        unit:      "om:degree-Celsius",
        readOnly:  true
      },

      // --- Configurações graváveis ---
      samplingIntervalMs: {
        title:       "Intervalo de amostragem (ms)",
        description: "Intervalo entre leituras dos sensores. Padrão: 5000 ms.",
        type:        "integer",
        unit:        "om:millisecond",
        minimum:     1000, maximum: 60000,
        default:     5000
      },
      emissivity: {
        title:       "Emissividade (MLX90614)",
        description: "Coeficiente de emissividade — pele humana ≈ 0.98.",
        type:        "number",
        minimum:     0.1, maximum: 1.0,
        default:     0.98
      },
      thresholds: {
        title:       "Limiares Clínicos Personalizados",
        description: "Limiares de BPM, SpO2 e temperatura para geração de alertas.",
        type:        "object",
        observable:  true,
        properties: {
          heartRate:       { type: "object" },
          spO2:            { type: "object" },
          bodyTemperature: { type: "object" }
        }
      },

      // --- Estado do atuador (só leitura) ---
      vibrationActive: {
        title:    "Vibração activa",
        description: "Indica se o motor de vibração está em funcionamento.",
        type:     "boolean",
        readOnly: true, observable: true
      },
      lastVibrationAt: {
        title:    "Última activação do atuador",
        type:     "string",
        format:   "date-time",
        readOnly: true
      },

      // --- Estado do dispositivo (só leitura) ---
      connectionStatus: {
        title:    "Estado de Conectividade",
        type:     "string",
        enum:     ["online", "offline", "reconnecting"],
        readOnly: true, observable: true
      },
      batteryLevel: {
        title:    "Nível de Bateria (%)",
        type:     "integer",
        unit:     "om:percent",
        minimum:  0, maximum: 100,
        readOnly: true, observable: true
      },
      patient: {
        title:    "Paciente associado",
        description: "Dados mínimos do paciente emparelhado com o dispositivo.",
        type:     "object",
        readOnly: true
      }
    },

    // ── Ações ──────────────────────────────────────────────────
    actions: {
      calibrate: {
        title:       "Calibrar sensores",
        description: "Rotina de calibração automática do MAX30100.",
        synchronous: false,
        input:  { type: "object", properties: { mode: { type: "string", enum: ["quick","full"], default: "quick" } } },
        output: { type: "object", properties: { success: { type: "boolean" }, durationMs: { type: "integer" } } }
      },
      activateVibration: {
        "@type":     "healthiot:HapticAlert",
        title:       "Activar vibração",
        description: "Activa o motor de vibração com duração, padrão e intensidade configuráveis.",
        synchronous: true,
        input: {
          type: "object",
          required: ["duration_ms", "pattern"],
          properties: {
            duration_ms: { type: "integer", minimum: 100, maximum: 10000 },
            pattern:     { type: "string",  enum: ["short","long","sos"], default: "short" },
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
        title:       "Parar vibração",
        description: "Interrompe imediatamente qualquer vibração em curso.",
        idempotent:  true
      },
      registerDevice: {
        title:       "Registar no Thing Directory",
        description: "Publica esta TD no Thing Directory Eclipse Thingweb.",
        input: { type: "object", properties: { directoryUrl: { type: "string", format: "uri" } } }
      }
    },

    // ── Eventos ────────────────────────────────────────────────
    events: {
      criticalHealthAlert: {
        "@type":     "healthiot:CriticalHealthAlert",
        title:       "Alerta Clínico Crítico",
        description: "Emitido quando um sensor viola um limiar (green/yellow/red).",
        data: { type: "object", required: ["source","value","severity","timestamp"] }
      },
      vibrationCompleted: {
        title:       "Vibração concluída",
        description: "Emitido quando um ciclo de vibração termina.",
        data: { type: "object" }
      },
      deviceStatusChanged: {
        title:       "Mudança de estado do dispositivo",
        description: "Emitido quando o dispositivo muda para online/offline ou bateria crítica.",
        data: { type: "object" }
      }
    }
  });

  // ================================================================
  //  HANDLERS DE LEITURA DE PROPRIEDADES  (RF — leitura de sensores)
  // ================================================================

  // Propriedades de sensores (apenas leitura)
  for (const key of [
    "heartRate", "spO2", "bodyTemperature", "ambientTemperature",
    "vibrationActive", "lastVibrationAt", "connectionStatus",
    "batteryLevel", "patient"
  ]) {
    thing.setPropertyReadHandler(key, async () => state[key]);
  }

  // ================================================================
  //  HANDLERS DE ESCRITA DE PROPRIEDADES  (RF — escrita de config.)
  // ================================================================

  // samplingIntervalMs — altera intervalo de amostragem
  thing.setPropertyReadHandler("samplingIntervalMs", async () => state.samplingIntervalMs);
  thing.setPropertyWriteHandler("samplingIntervalMs", async (val) => {
    const novo = await val.value();
    if (novo < 1000 || novo > 60000) throw new Error("Valor fora do intervalo [1000, 60000].");
    state.samplingIntervalMs = novo;
    console.log(`⚙️  samplingIntervalMs → ${novo} ms`);
  });

  // emissivity — calibração do sensor IR
  thing.setPropertyReadHandler("emissivity", async () => state.emissivity);
  thing.setPropertyWriteHandler("emissivity", async (val) => {
    const novo = await val.value();
    if (novo < 0.1 || novo > 1.0) throw new Error("Emissividade fora do intervalo [0.1, 1.0].");
    state.emissivity = novo;
    console.log(`⚙️  emissivity → ${novo}`);
  });

  // thresholds — limiares clínicos personalizados (escrita de dados)
  thing.setPropertyReadHandler("thresholds", async () => state.thresholds);
  thing.setPropertyWriteHandler("thresholds", async (val) => {
    const novo = await val.value();
    // Merge parcial: permite actualizar apenas algumas secções
    if (novo.heartRate)       state.thresholds.heartRate       = { ...state.thresholds.heartRate,       ...novo.heartRate };
    if (novo.spO2)            state.thresholds.spO2            = { ...state.thresholds.spO2,            ...novo.spO2 };
    if (novo.bodyTemperature) state.thresholds.bodyTemperature = { ...state.thresholds.bodyTemperature, ...novo.bodyTemperature };
    console.log("⚙️  thresholds actualizados:", JSON.stringify(state.thresholds));
  });

  // ================================================================
  //  HANDLER DA AÇÃO — activateVibration  (atuador háptico)
  // ================================================================

  thing.setActionHandler("activateVibration", async (params) => {
    const input = await params.value();
    const { duration_ms, pattern = "short", intensity = 200 } = input;

    if (state.vibrationActive) {
      // Para a vibração anterior antes de iniciar nova
      clearTimeout(state.vibrationTimer);
    }

    state.vibrationActive  = true;
    state.lastVibrationAt  = new Date().toISOString();
    console.log(`📳 activateVibration | pattern=${pattern} | duration=${duration_ms}ms | intensity=${intensity}`);

    // Agenda o fim da vibração
    state.vibrationTimer = setTimeout(() => {
      state.vibrationActive = false;
      const completedPayload = {
        durationMs: duration_ms,
        pattern,
        timestamp: new Date().toISOString()
      };
      thing.emitEvent("vibrationCompleted", completedPayload);
      console.log(`📳 vibração terminada (${pattern}, ${duration_ms}ms)`);
    }, duration_ms);

    return { success: true, startedAt: state.lastVibrationAt };
  });

  // ── Outros handlers de ações ───────────────────────────────

  thing.setActionHandler("calibrate", async (params) => {
    const input = params ? await params.value() : {};
    const modo = input?.mode ?? "quick";
    const duracao = modo === "full" ? 5000 : 1200;
    console.log(`🛠  calibrate [${modo}] — ${duracao}ms`);
    return { success: true, durationMs: duracao };
  });

  thing.setActionHandler("stopVibration", async () => {
    if (state.vibrationTimer) clearTimeout(state.vibrationTimer);
    state.vibrationActive = false;
    console.log("🛑 stopVibration");
  });

  thing.setActionHandler("registerDevice", async (params) => {
    const input = params ? await params.value() : {};
    const url = input?.directoryUrl ?? "https://gateway.wot-health.local:8081/things";
    console.log(`📒 registerDevice → ${url}`);
    return { success: true };
  });

  // ================================================================
  //  SIMULAÇÃO PERIÓDICA DOS SENSORES  (substitui dados do ESP32)
  // ================================================================

  let simInterval = null;

  function iniciarSimulacao() {
    if (simInterval) clearInterval(simInterval);
    simInterval = setInterval(() => {
      // Atualiza valores simulados dos sensores
      state.heartRate       = Math.floor(55 + Math.random() * 75); // 55–130 BPM
      state.spO2            = parseFloat((93 + Math.random() * 7).toFixed(1));
      state.bodyTemperature = parseFloat((35.5 + Math.random() * 3).toFixed(2));
      state.batteryLevel    = Math.max(0, parseFloat((state.batteryLevel - 0.02).toFixed(2)));

      console.log(
        `💓 BPM:${state.heartRate}` +
        ` | SpO2:${state.spO2}%` +
        ` | Temp:${state.bodyTemperature}°C` +
        ` | Bat:${state.batteryLevel}%`
      );

      // ── MECANISMO DE ALERTA BASEADO EM REGRAS ─────────────────
      // Avalia todos os limiares e emite eventos criticalHealthAlert
      const alertas = avaliarLimiares(thing);
      for (const alerta of alertas) {
        console.log(`🚨 ALERTA [${alerta.severity.toUpperCase()}] ${alerta.source}=${alerta.value}`);
        thing.emitEvent("criticalHealthAlert", alerta);

        // Disparo automático de vibração em alertas vermelhos
        if (alerta.severity === "red" && !state.vibrationActive) {
          console.log("📳 Auto-vibração por alerta crítico (SOS)");
          state.vibrationActive = true;
          state.lastVibrationAt = new Date().toISOString();
          state.vibrationTimer  = setTimeout(() => {
            state.vibrationActive = false;
            thing.emitEvent("vibrationCompleted", {
              durationMs: 3000,
              pattern:    "sos",
              timestamp:  new Date().toISOString()
            });
          }, 3000);
        }
      }
    }, state.samplingIntervalMs);
  }

  iniciarSimulacao();

  // Re-arranca o intervalo quando samplingIntervalMs muda
  const origWrite = thing.getPropertyWriteHandler?.("samplingIntervalMs");
  thing.setPropertyWriteHandler("samplingIntervalMs", async (val) => {
    const novo = await val.value();
    if (novo < 1000 || novo > 60000) throw new Error("Valor fora do intervalo [1000, 60000].");
    state.samplingIntervalMs = novo;
    console.log(`⚙️  samplingIntervalMs → ${novo} ms (a reiniciar simulação)`);
    iniciarSimulacao();
  });

  // Emite deviceStatusChanged periodicamente
  setInterval(() => {
    thing.emitEvent("deviceStatusChanged", {
      connectionStatus: state.connectionStatus,
      batteryLevel:     state.batteryLevel,
      timestamp:        new Date().toISOString()
    });
  }, 10000);

  // ── Expõe a Thing ──────────────────────────────────────────
  await thing.expose();

  console.log("\n🚀 Servient Producer ativo (Raspberry Pi 4):");
  console.log("   • Thing Description  → GET  http://localhost:8080/patient001");
  console.log("   • heartRate          → GET  http://localhost:8080/patient001/properties/heartRate");
  console.log("   • spO2               → GET  http://localhost:8080/patient001/properties/spO2");
  console.log("   • bodyTemperature    → GET  http://localhost:8080/patient001/properties/bodyTemperature");
  console.log("   • samplingIntervalMs → PUT  http://localhost:8080/patient001/properties/samplingIntervalMs");
  console.log("   • thresholds         → PUT  http://localhost:8080/patient001/properties/thresholds");
  console.log("   • activateVibration  → POST http://localhost:8080/patient001/actions/activateVibration");
  console.log("   • stopVibration      → POST http://localhost:8080/patient001/actions/stopVibration");
  console.log("   • calibrate          → POST http://localhost:8080/patient001/actions/calibrate");
  console.log("   • criticalHealthAlert→ SSE  http://localhost:8080/patient001/events/criticalHealthAlert");

}).catch((err) => {
  console.error("❌ Erro ao iniciar o Servient Producer:", err);
  process.exit(1);
});
