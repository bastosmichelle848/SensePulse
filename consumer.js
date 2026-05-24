// consumer.js — Consome a Thing exposta pelo server.js
// Pede a TD ao próprio servient (em vez de ler o ficheiro estático),
// para que as URLs das "forms" sejam geradas correctamente pelo node-wot.
//
// Como correr (depois do server.js já estar a correr):
//   npm install @node-wot/core @node-wot/binding-http
//   # opcional: npm install @node-wot/binding-mqtt
//   node consumer.js

const { Servient, Helpers } = require("@node-wot/core");
const { HttpClientFactory } = require("@node-wot/binding-http");
// const { MqttClientFactory } = require("@node-wot/binding-mqtt"); // opcional

const servient = new Servient();
servient.addClientFactory(new HttpClientFactory());
// servient.addClientFactory(new MqttClientFactory()); // opcional

servient.start().then(async (WoT) => {

  // 🔑 A chave: vamos BUSCAR a TD ao servient em vez de ler o JSON local.
  //    Assim os "forms" apontam para as URLs reais (/patient001/properties/...).
  let td;
  if (typeof WoT.requestThingDescription === "function") {
    // node-wot ≥ 0.8.x
    td = await WoT.requestThingDescription("http://localhost:8080/patient001");
  } else {
    // fallback para versões antigas
    const helpers = new Helpers(servient);
    td = await helpers.fetch("http://localhost:8080/patient001");
  }

  const thing = await WoT.consume(td);
  console.log("✅ Thing consumida:", td.title);

  // Leitura periódica de várias propriedades
  setInterval(async () => {
    try {
      const [hr, spo2, temp] = await Promise.all([
        thing.readProperty("heartRate"),
        thing.readProperty("spO2"),
        thing.readProperty("bodyTemperature")
      ]);
      console.log(
        "💓 BPM:",   await hr.value(),
        "| SpO2:",   await spo2.value(),
        "| Temp:",   await temp.value()
      );
    } catch (err) {
      console.error("❌ erro na leitura:", err.message);
    }
  }, 2000);

  // Exemplo de invocação de ação (descomenta para testar)
  // setTimeout(async () => {
  //   const out = await thing.invokeAction("activateVibration",
  //     { duration_ms: 1500, pattern: "short", intensity: 180 });
  //   console.log("📳 resposta:", await out.value());
  // }, 5000);

  // Exemplo de subscrição a evento (HTTP long-poll / SSE)
  // await thing.subscribeEvent("criticalHealthAlert", async (data) => {
  //   console.log("🚨 ALERTA:", await data.value());
  // });
});