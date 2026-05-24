/**
 * ============================================================
 *  SERVIENT CONSUMER — Wearable Health Monitor (patient001)
 * ============================================================
 *
 *  NÃO faz nada enquanto deviceReady = false.
 *  Toda a lógica de leitura, escrita e ações está bloqueada
 *  até o ESP32 estar ligado e a publicar dados reais via MQTT.
 *
 *  Como correr:
 *    npm install @node-wot/core @node-wot/binding-http
 *    node client_consumer.js
 * ============================================================
 */

"use strict";

const { Servient, Helpers } = require("@node-wot/core");
const { HttpClientFactory } = require("@node-wot/binding-http");

const PRODUCER_URL  = "http://localhost:8080/patient001";
const POLL_MS       = 3000;   // intervalo de verificação de deviceReady
const LEITURA_MS    = 5000;   // intervalo de leitura periódica

const servient = new Servient();
servient.addClientFactory(new HttpClientFactory());

const local = {
  leituraCount:       0,
  alertasRecebidos:   0,
  vibracoesCompletas: 0,
  autoRespostaAtiva:  false,
  loopHandle:         null
};

function ts() {
  return new Date().toLocaleTimeString("pt-PT");
}

// ════════════════════════════════════════════════════════════
//  LEITURA DE PROPRIEDADES
// ════════════════════════════════════════════════════════════

async function lerSensoresPrincipais(thing) {
  const [hrI, spo2I, tempI] = await Promise.all([
    thing.readProperty("heartRate"),
    thing.readProperty("spO2"),
    thing.readProperty("bodyTemperature")
  ]);
  const hr   = await hrI.value();
  const spo2 = await spo2I.value();
  const temp = await tempI.value();

  local.leituraCount++;
  console.log(
    `[${ts()}] 📊 Leitura #${local.leituraCount}` +
    ` | 💓 BPM: ${hr}` +
    ` | 🩸 SpO2: ${spo2}%` +
    ` | 🌡️  Temp: ${temp ?? "---"}°C`
  );
  return { hr, spo2, temp };
}

async function lerEstadoDispositivo(thing) {
  const [batI, connI, vibrI] = await Promise.all([
    thing.readProperty("batteryLevel"),
    thing.readProperty("connectionStatus"),
    thing.readProperty("vibrationActive")
  ]);
  const bat  = await batI.value();
  const conn = await connI.value();
  const vibr = await vibrI.value();

  console.log(
    `[${ts()}] 🔌 Estado` +
    ` | Ligação: ${conn}` +
    ` | 🔋 Bateria: ${bat ?? "---"}%` +
    ` | 📳 Vibração: ${vibr ? "ATIVA" : "inativa"}`
  );
}

// ════════════════════════════════════════════════════════════
//  ESCRITA DE PROPRIEDADES
// ════════════════════════════════════════════════════════════

async function escreverLimiares(thing, limiares) {
  await thing.writeProperty("thresholds", limiares);
  console.log(`[${ts()}] ⚙️  thresholds → ${JSON.stringify(limiares)}`);
}

async function escreverIntervaloAmostragem(thing, ms) {
  await thing.writeProperty("samplingIntervalMs", ms);
  console.log(`[${ts()}] ⚙️  samplingIntervalMs → ${ms} ms`);
}

async function escreverEmissividade(thing, valor) {
  await thing.writeProperty("emissivity", valor);
  console.log(`[${ts()}] ⚙️  emissivity → ${valor}`);
}

// ════════════════════════════════════════════════════════════
//  INVOCAÇÃO DE AÇÕES
// ════════════════════════════════════════════════════════════

async function invocarActivarVibracao(thing, duration_ms, pattern, intensity) {
  console.log(`[${ts()}] 📳 activateVibration → pattern=${pattern} | ${duration_ms}ms`);
  const out = await thing.invokeAction("activateVibration", { duration_ms, pattern, intensity });
  const res = await out.value();
  console.log(`[${ts()}] 📳 Iniciada: success=${res.success} | startedAt=${res.startedAt}`);
  return res;
}

async function invocarCalibrar(thing, mode) {
  console.log(`[${ts()}] 🛠  calibrate [${mode}]`);
  const out = await thing.invokeAction("calibrate", { mode });
  const res = await out.value();
  console.log(`[${ts()}] 🛠  Concluída: durationMs=${res.durationMs}`);
}

// ════════════════════════════════════════════════════════════
//  SUBSCRIÇÃO A EVENTOS
// ════════════════════════════════════════════════════════════

async function subscreverEventos(thing) {
  await thing.subscribeEvent("criticalHealthAlert", async (dados) => {
    const a = await dados.value();
    local.alertasRecebidos++;
    const label = a.severity === "red" ? "🔴 CRÍTICO" : "🟡 ATENÇÃO";
    console.log(
      `\n[${ts()}] 🚨 ALERTA #${local.alertasRecebidos} ${label}` +
      ` | ${a.source} = ${a.value}` +
      ` | threshold: ${JSON.stringify(a.threshold)}\n`
    );
    if (a.severity === "red" && !local.autoRespostaAtiva) {
      local.autoRespostaAtiva = true;
      console.log(`[${ts()}] 🤖 Auto-resposta: vibração SOS`);
      await invocarActivarVibracao(thing, 2500, "sos", 255);
      setTimeout(() => { local.autoRespostaAtiva = false; }, 4000);
    }
  });
  console.log(`[${ts()}] ✅ Subscrito: criticalHealthAlert`);

  await thing.subscribeEvent("vibrationCompleted", async (dados) => {
    const ev = await dados.value();
    local.vibracoesCompletas++;
    console.log(`[${ts()}] 📳 vibrationCompleted #${local.vibracoesCompletas} | pattern=${ev.pattern} | ${ev.durationMs}ms`);
  });
  console.log(`[${ts()}] ✅ Subscrito: vibrationCompleted`);

  await thing.subscribeEvent("deviceStatusChanged", async (dados) => {
    const ev = await dados.value();
    console.log(`[${ts()}] 🔌 deviceStatusChanged | status=${ev.connectionStatus} | mqtt=${ev.mqttConectado}`);
    if (ev.connectionStatus === "offline" && local.loopHandle) {
      clearInterval(local.loopHandle);
      local.loopHandle = null;
      console.warn(`[${ts()}] ⚠️  ESP32 offline — loop de leituras parado`);
    }
  });
  console.log(`[${ts()}] ✅ Subscrito: deviceStatusChanged`);
}

// ════════════════════════════════════════════════════════════
//  SEQUÊNCIA DE ARRANQUE — só corre uma vez, quando ready=true
// ════════════════════════════════════════════════════════════

async function arrancarConsumer(thing) {
  console.log(`\n[${ts()}] 🚀 ESP32 pronto — a iniciar Consumer\n`);

  // 1. Leitura do paciente
  const patI = await thing.readProperty("patient");
  const pat  = await patI.value();
  console.log(`[${ts()}] 👤 ${pat.displayName} | id=${pat.patientId} | idade=${pat.age}`);

  // 2. Escrita de configurações
  console.log(`\n[${ts()}] --- CONFIGURAÇÃO ---`);
  await escreverIntervaloAmostragem(thing, 5000);
  await escreverEmissividade(thing, 0.98);
  await escreverLimiares(thing, {
    heartRate:       { min: 55, max: 130 },
    spO2:            { min: 92 },
    bodyTemperature: { min: 35.5, max: 38.0 }
  });

  // 3. Calibração
  console.log(`\n[${ts()}] --- CALIBRAÇÃO ---`);
  await invocarCalibrar(thing, "quick");

  // 4. Teste do atuador
  console.log(`\n[${ts()}] --- TESTE DO ATUADOR ---`);
  await invocarActivarVibracao(thing, 600, "short", 180);

  // 5. Loop de leituras periódicas
  console.log(`\n[${ts()}] --- LEITURAS PERIÓDICAS (a cada ${LEITURA_MS / 1000}s) ---\n`);
  let ciclo = 0;
  local.loopHandle = setInterval(async () => {
    ciclo++;
    try {
      await lerSensoresPrincipais(thing);
      if (ciclo % 5 === 0) await lerEstadoDispositivo(thing);
    } catch (err) {
      console.error(`[${ts()}] ❌ Erro no loop: ${err.message}`);
    }
  }, LEITURA_MS);
}

// ════════════════════════════════════════════════════════════
//  ARRANQUE PRINCIPAL
// ════════════════════════════════════════════════════════════
servient.start().then(async (WoT) => {

  console.log("🔌 Consumer a arrancar...");

  let td;
  try {
    if (typeof WoT.requestThingDescription === "function") {
      td = await WoT.requestThingDescription(PRODUCER_URL);
    } else {
      const helpers = new Helpers(servient);
      td = await helpers.fetch(PRODUCER_URL);
    }
  } catch (err) {
    console.error(`❌ Não foi possível obter a TD: ${err.message}`);
    console.error("   Confirma que o server_producer.js está a correr.");
    process.exit(1);
  }

  const thing = await WoT.consume(td);
  console.log(`✅ Thing Description obtida: "${td.title}"\n`);

  // Subscreve eventos imediatamente (independente do estado do ESP32)
  console.log("--- SUBSCRIÇÃO A EVENTOS ---");
  await subscreverEventos(thing);

  // Polling de deviceReady — NÃO faz mais nada enquanto for false
  console.log(`\n[${ts()}] ⏳ À espera que o ESP32 esteja ligado ...\n`);

  let jaArrancou = false;

  const pollHandle = setInterval(async () => {
    let pronto = false;
    try {
      const inter = await thing.readProperty("deviceReady");
      pronto = await inter.value();
    } catch {
      // Producer ainda não acessível — silêncio
      return;
    }

    if (!pronto) {
      process.stdout.write(`\r[${ts()}] ⏳ ESP32 não ligado (deviceReady = false) ...`);
      return;
    }

    // deviceReady = true: limpa o poll e arranca uma única vez
    if (!jaArrancou) {
      jaArrancou = true;
      clearInterval(pollHandle);
      process.stdout.write("\n");
      await arrancarConsumer(thing);

      // Resumo periódico
      setInterval(() => {
        console.log(
          `\n[${ts()}] 📈 RESUMO` +
          ` | Leituras: ${local.leituraCount}` +
          ` | Alertas: ${local.alertasRecebidos}` +
          ` | Vibrações: ${local.vibracoesCompletas}\n`
        );
      }, 60000);
    }
  }, POLL_MS);

}).catch((err) => {
  console.error("❌ Erro ao iniciar o Consumer:", err);
  process.exit(1);
});
