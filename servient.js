const { Servient } = require("@node-wot/core");
const { HttpServer } = require("@node-wot/binding-http");

const td = require("./TD_Unified.json");

const servient = new Servient();
servient.addServer(new HttpServer({ port: 8080 }));

servient.start().then(async (WoT) => {

  const thing = await WoT.produce(td);

  console.log(" Wearable Health Monitor ativo!");

  // =========================
  // SIMULAÇÃO DE SENSORES
  // =========================
  let heartRate = 75;
  let spO2 = 98;
  let bodyTemp = 36.6;
  let battery = 85;
  let connection = "online";

  setInterval(() => {
    heartRate = 60 + Math.random() * 40;
    spO2 = 94 + Math.random() * 6;
    bodyTemp = 36 + Math.random() * 2;
    battery = Math.max(0, battery - 0.05);
  }, 2000);

  // =========================
  //  PROPERTIES (sensores)
  // =========================

  thing.setPropertyReadHandler("heartRate", async () => heartRate);

  thing.setPropertyReadHandler("spO2", async () => spO2);

  thing.setPropertyReadHandler("bodyTemperature", async () => bodyTemp);

  thing.setPropertyReadHandler("batteryLevel", async () => battery);

  thing.setPropertyReadHandler("connectionStatus", async () => connection);

  thing.setPropertyReadHandler("ambientTemperature", async () => 25.0);

  thing.setPropertyReadHandler("vibrationActive", async () => false);

  thing.setPropertyReadHandler("lastVibrationAt", async () => new Date().toISOString());

  thing.setPropertyReadHandler("patient", async () => ({
    patientId: "patient001",
    displayName: "Paciente Demo",
    age: 21
  }));

  // thresholds (simples)
  thing.setPropertyReadHandler("thresholds", async () => ({
    heartRate: { min: 50, max: 150 },
    spO2: { min: 90 },
    bodyTemperature: { min: 35.0, max: 38.0 }
  }));

  // =========================
  // ACTIONS
  // =========================

  thing.setActionHandler("calibrate", async (input) => {
    console.log(" Calibração:", input);
    return {
      success: true,
      durationMs: 1200
    };
  });

  thing.setActionHandler("activateVibration", async (input) => {
    console.log(" Vibração ativada:", input);

    return {
      success: true,
      startedAt: new Date().toISOString()
    };
  });

  thing.setActionHandler("stopVibration", async () => {
    console.log(" Vibração parada");
    return true;
  });

  thing.setActionHandler("registerDevice", async (input) => {
    console.log("Registo no diretório:", input?.directoryUrl);
    return { success: true };
  });

  // =========================
  //  EVENTOS (simulação)
  // =========================

  setInterval(() => {

    if (heartRate > 140) {
      thing.emitEvent("criticalHealthAlert", {
        source: "heartRate",
        value: heartRate,
        severity: "red",
        patientId: "patient001",
        timestamp: new Date().toISOString()
      });
    }

  }, 3000);

  setInterval(() => {
    thing.emitEvent("deviceStatusChanged", {
      connectionStatus: "online",
      batteryLevel: battery,
      timestamp: new Date().toISOString()
    });
  }, 5000);

  // =========================
  //  EXPOR THING
  // =========================

  await thing.expose();

  console.log("🌐 Thing exposta em http://localhost:8080");
});