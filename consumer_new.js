/**
 * ============================================================
 *  SERVIENT CONSUMER — Wearable Health Monitor (patient001)
 *  Hardware: PC/Servidor de Monitorização (Dashboard Node.js)
 * ============================================================
 *
 *  Consome a Thing exposta pelo server.js (Raspberry Pi 4).
 *  Demonstra:
 *    • Leitura de propriedades de sensores (heartRate, spO2, bodyTemperature)
 *    • Escrita de propriedades de configuração (thresholds, samplingIntervalMs)
 *    • Invocação de ação de alteração de estado do atuador (activateVibration)
 *    • Subscrição de eventos (criticalHealthAlert, deviceStatusChanged)
 *
 *  Como correr (com o server.js já em execução):
 *    npm install @node-wot/core @node-wot/binding-http
 *    node consumer.js
 * ============================================================
 */

"use strict";

const { Servient, Helpers } = require("@node-wot/core");
const { HttpClientFactory } = require("@node-wot/binding-http");
// const { MqttClientFactory } = require("@node-wot/binding-mqtt");

// ─── URL do Servient Producer ────────────────────────────────
const PRODUCER_BASE_URL = "http://localhost:8080";
const THING_ID          = "patient001";
const TD_URL            = `${PRODUCER_BASE_URL}/${THING_ID}`;

// ─── Inicialização do Consumer ────────────────────────────────
const servient = new Servient();
servient.addClientFactory(new HttpClientFactory());
// servient.addClientFactory(new MqttClientFactory());

// ─── Utilitários de log ───────────────────────────────────────
const log = {
  info:  (msg) => console.log(`[${new Date().toISOString()}] ℹ️  ${msg}`),
  ok:    (msg) => console.log(`[${new Date().toISOString()}] ✅  ${msg}`),
  warn:  (msg) => console.warn(`[${new Date().toISOString()}] ⚠️  ${msg}`),
  error: (msg) => console.error(`[${new Date().toISOString()}] ❌  ${msg}`),
  alert: (msg) => console.log(`[${new Date().toISOString()}] 🚨  ${msg}`),
  data:  (msg) => console.log(`[${new Date().toISOString()}] 📊  ${msg}`)
};

// ────────────────────────────────────────────────────────────────
servient.start().then(async (WoT) => {

  // ── 1. Obter a Thing Description do Producer ────────────────
  log.info(`A obter TD de ${TD_URL} ...`);
  let td;
  try {
    if (typeof WoT.requestThingDescription === "function") {
      // node-wot ≥ 0.8.x
      td = await WoT.requestThingDescription(TD_URL);
    } else {
      const helpers = new Helpers(servient);
      td = await helpers.fetch(TD_URL);
    }
  } catch (err) {
    log.error(`Não foi possível obter a TD: ${err.message}`);
    log.warn("Certifica-te de que o server.js está em execução em localhost:8080");
    process.exit(1);
  }

  const thing = await WoT.consume(td);
  log.ok(`Thing consumida: "${td.title}" (${td.id})`);

  // ================================================================
  //  A) LEITURA DE PROPRIEDADES DOS SENSORES
  // ================================================================

  /**
   * lerPropriedadeSensor — leitura unitária com tratamento de erros.
   * Retorna o valor ou null em caso de falha.
   */
  async function lerPropriedadeSensor(nome) {
    try {
      const interactionOutput = await thing.readProperty(nome);
      return await interactionOutput.value();
    } catch (err) {
      log.error(`Erro ao ler "${nome}": ${err.message}`);
      return null;
    }
  }

  /**
   * lerTodosSensores — leitura simultânea de todas as propriedades
   * de sensores usando Promise.allSettled (tolerante a falhas parciais).
   */
  async function lerTodosSensores() {
    const nomes = ["heartRate", "spO2", "bodyTemperature", "batteryLevel", "connectionStatus"];
    const resultados = await Promise.allSettled(
      nomes.map(n => lerPropriedadeSensor(n))
    );

    const valores = {};
    nomes.forEach((nome, i) => {
      valores[nome] = resultados[i].status === "fulfilled" ? resultados[i].value : null;
    });
    return valores;
  }

  // Leitura periódica de sensores (a cada 3 s)
  setInterval(async () => {
    const dados = await lerTodosSensores();
    log.data(
      `BPM:${dados.heartRate ?? "?"} bpm` +
      ` | SpO2:${dados.spO2 ?? "?"}%` +
      ` | Temp:${dados.bodyTemperature ?? "?"}°C` +
      ` | Bat:${dados.batteryLevel ?? "?"}%` +
      ` | Estado:${dados.connectionStatus ?? "?"}`
    );
  }, 3000);

  // ── Leitura individual demonstrativa ──────────────────────────
  const hrInicial = await lerPropriedadeSensor("heartRate");
  log.ok(`Leitura inicial — Frequência cardíaca: ${hrInicial} bpm`);

  const paciente = await lerPropriedadeSensor("patient");
  if (paciente) {
    log.ok(`Paciente: ${paciente.displayName} (ID: ${paciente.patientId}, Idade: ${paciente.age})`);
  }

  // ================================================================
  //  B) ESCRITA DE PROPRIEDADES (configuração do dispositivo)
  // ================================================================

  /**
   * escreverPropriedade — escrita com log e tratamento de erros.
   */
  async function escreverPropriedade(nome, valor) {
    try {
      await thing.writeProperty(nome, valor);
      log.ok(`Propriedade "${nome}" actualizada → ${JSON.stringify(valor)}`);
      return true;
    } catch (err) {
      log.error(`Erro ao escrever "${nome}": ${err.message}`);
      return false;
    }
  }

  // Exemplo B1: alterar o intervalo de amostragem para 3 segundos
  setTimeout(async () => {
    await escreverPropriedade("samplingIntervalMs", 3000);
  }, 5000);

  // Exemplo B2: actualizar limiares clínicos do paciente
  setTimeout(async () => {
    await escreverPropriedade("thresholds", {
      heartRate:       { min: 50, max: 140 },
      spO2:            { min: 92 },
      bodyTemperature: { min: 35.5, max: 38.0 }
    });
  }, 8000);

  // Verificação após escrita: releitura dos limiares
  setTimeout(async () => {
    const thresholds = await lerPropriedadeSensor("thresholds");
    if (thresholds) {
      log.ok(`Limiares activos: ${JSON.stringify(thresholds)}`);
    }
  }, 9500);

  // ================================================================
  //  C) INVOCAÇÃO DE AÇÃO — activateVibration (atuador háptico)
  // ================================================================

  /**
   * activarVibracao — invoca a ação sobre o atuador de vibração
   * com os parâmetros especificados.
   */
  async function activarVibracao(duration_ms, pattern, intensity) {
    try {
      log.info(`A invocar activateVibration (${pattern}, ${duration_ms}ms, intensity=${intensity})...`);
      const output = await thing.invokeAction("activateVibration", {
        duration_ms,
        pattern,
        intensity
      });
      const resultado = output ? await output.value() : null;
      log.ok(`activateVibration → sucesso=${resultado?.success}, iniciado em=${resultado?.startedAt}`);
      return resultado;
    } catch (err) {
      log.error(`Erro ao invocar activateVibration: ${err.message}`);
      return null;
    }
  }

  // Invocação de demonstração: vibração curta ao fim de 12 segundos
  setTimeout(async () => {
    await activarVibracao(1500, "short", 180);
  }, 12000);

  // Invocação de ação de calibração
  setTimeout(async () => {
    try {
      const out = await thing.invokeAction("calibrate", { mode: "quick" });
      const res = out ? await out.value() : null;
      log.ok(`calibrate → ${JSON.stringify(res)}`);
    } catch (err) {
      log.error(`Erro ao calibrar: ${err.message}`);
    }
  }, 15000);

  // ================================================================
  //  D) SUBSCRIÇÃO DE EVENTOS
  // ================================================================

  // ── D1: criticalHealthAlert — alerta clínico ───────────────────
  try {
    await thing.subscribeEvent("criticalHealthAlert", async (interactionOutput) => {
      const alerta = await interactionOutput.value();
      const sev    = (alerta.severity ?? "?").toUpperCase();
      const emoji  = sev === "RED" ? "🔴" : sev === "YELLOW" ? "🟡" : "🟢";

      log.alert(
        `${emoji} ALERTA CLÍNICO [${sev}]` +
        ` | sensor=${alerta.source}` +
        ` | valor=${alerta.value}` +
        ` | paciente=${alerta.patientId}` +
        ` | ts=${alerta.timestamp}`
      );

      // Acção automática: activar vibração SOS em alertas críticos vermelhos
      if (alerta.severity === "red") {
        log.warn("Alerta vermelho detectado — a activar vibração SOS automática...");
        await activarVibracao(3000, "sos", 255);
      }
    });
    log.ok("Subscrito ao evento criticalHealthAlert");
  } catch (err) {
    log.error(`Não foi possível subscrever criticalHealthAlert: ${err.message}`);
  }

  // ── D2: deviceStatusChanged — estado do dispositivo ───────────
  try {
    await thing.subscribeEvent("deviceStatusChanged", async (interactionOutput) => {
      const estado = await interactionOutput.value();
      const conexao = estado.connectionStatus ?? "?";
      const bateria = estado.batteryLevel ?? "?";

      if (conexao === "offline") {
        log.warn(`Dispositivo OFFLINE — última bateria: ${bateria}%`);
      } else {
        log.info(`Estado do dispositivo: ${conexao} | Bateria: ${bateria}%`);
      }

      // Alerta de bateria baixa (< 20%)
      if (typeof bateria === "number" && bateria < 20) {
        log.warn(`⚡ Bateria crítica: ${bateria}% — recarga necessária!`);
      }
    });
    log.ok("Subscrito ao evento deviceStatusChanged");
  } catch (err) {
    log.error(`Não foi possível subscrever deviceStatusChanged: ${err.message}`);
  }

  // ── D3: vibrationCompleted — confirmação do atuador ───────────
  try {
    await thing.subscribeEvent("vibrationCompleted", async (interactionOutput) => {
      const dados = await interactionOutput.value();
      log.ok(`Vibração concluída — padrão=${dados.pattern}, duração=${dados.durationMs}ms`);
    });
    log.ok("Subscrito ao evento vibrationCompleted");
  } catch (err) {
    log.error(`Não foi possível subscrever vibrationCompleted: ${err.message}`);
  }

  // ================================================================
  //  E) LEITURA ÚNICA DE DIAGNÓSTICO (ao iniciar)
  // ================================================================
  setTimeout(async () => {
    log.info("--- Diagnóstico completo do dispositivo ---");
    const props = [
      "heartRate", "spO2", "bodyTemperature", "ambientTemperature",
      "samplingIntervalMs", "emissivity", "vibrationActive",
      "lastVibrationAt", "batteryLevel", "connectionStatus"
    ];
    for (const prop of props) {
      const val = await lerPropriedadeSensor(prop);
      log.info(`  ${prop.padEnd(22)} = ${JSON.stringify(val)}`);
    }
    log.info("--- Fim do diagnóstico ---");
  }, 2500);

  log.ok("Servient Consumer em execução (PC / Servidor de Monitorização).");
  log.info("A aguardar dados do dispositivo...\n");

}).catch((err) => {
  console.error("❌ Erro ao iniciar o Servient Consumer:", err);
  process.exit(1);
});
