/**
 * ============================================================
 *  SERVIENT PRODUCER — Wearable Health Monitor (patient001)
 *  + REST API (Express :3001)
 *  + Base de Dados SQLite (health_monitor.db)
 * ============================================================
 *  Portas:
 *    :8080 → node-wot (Thing Description + affordances WoT)
 *    :3001 → REST API + dashboard.html
 *
 *  npm install @node-wot/core @node-wot/binding-http mqtt express sqlite3 cors
 *  node producer_server.js
 * ============================================================
 */
"use strict";

const { Servient } = require("@node-wot/core");
const { HttpServer } = require("@node-wot/binding-http");
const mqtt    = require("mqtt");
const express = require("express");
const sqlite3 = require("sqlite3").verbose();
const cors    = require("cors");
const path    = require("path");

const MQTT_BROKER    = "mqtt://test.mosquitto.org:1883";
const TOPIC_SENSOR   = "healthsensor";
const TOPIC_BEAT     = "healthsensor/beat";
const TOPIC_QUEDA    = "healthsensor/queda";
const MQTT_CLIENT_ID = `wot-producer-${Math.random().toString(16).slice(2,8)}`;
const COOLDOWN_QUEDA_MS = 30000;
let   tsUltimaQuedaWoT  = 0;

// ── BD SQLite ──────────────────────────────────────────────────────────────
const db = new sqlite3.Database("health_monitor.db", err => {
  if (err) { console.error("Erro BD:", err.message); process.exit(1); }
  console.log("BD SQLite: health_monitor.db");
});
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS bd_health (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    heartRate INTEGER NOT NULL, spO2 REAL NOT NULL,
    timestamp TEXT NOT NULL DEFAULT (datetime('now')))`);
  db.run(`CREATE TABLE IF NOT EXISTS bd_quedas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo TEXT NOT NULL, intensidade TEXT NOT NULL,
    contagem INTEGER NOT NULL, patientId TEXT NOT NULL,
    timestamp TEXT NOT NULL DEFAULT (datetime('now')))`);
  console.log("Tabelas: bd_health, bd_quedas");
});

function guardarLeitura(hr, spo2) {
  db.run("INSERT INTO bd_health (heartRate,spO2,timestamp) VALUES(?,?,datetime('now'))",
    [hr, spo2], err => { if(err) console.error("BD health:", err.message); });
}
function guardarQueda(tipo, intensidade, contagem, patientId) {
  db.run("INSERT INTO bd_quedas (tipo,intensidade,contagem,patientId,timestamp) VALUES(?,?,?,?,datetime('now'))",
    [tipo, intensidade, contagem, patientId], err => { if(err) console.error("BD queda:", err.message); });
}

// ── REST API Express :3001 ─────────────────────────────────────────────────
const app = express();
app.use(cors()); app.use(express.json());
app.use(express.static(path.join(__dirname)));
app.get("/", (req,res) => res.sendFile(path.join(__dirname,"dashboard.html")));

// Sinais vitais
app.get("/api/readings", (req,res) => {
  const limit = Math.min(parseInt(req.query.limit)||50,500);
  const from = req.query.from||"2000-01-01", to = req.query.to||"2099-01-01";
  db.all("SELECT * FROM bd_health WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp DESC LIMIT ?",
    [from,to,limit], (err,rows) => err ? res.status(500).json({error:err.message}) : res.json(rows));
});
app.get("/api/readings/latest", (req,res) => {
  db.get("SELECT * FROM bd_health ORDER BY timestamp DESC LIMIT 1",
    (err,row) => err ? res.status(500).json({error:err.message}) : res.json(row||{}));
});
app.post("/api/readings", (req,res) => {
  const {heartRate,spO2} = req.body;
  if(!heartRate||!spO2) return res.status(400).json({error:"heartRate e spO2 obrigatórios"});
  db.run("INSERT INTO bd_health (heartRate,spO2,timestamp) VALUES(?,?,datetime('now'))",
    [heartRate,spO2], function(err) {
      err ? res.status(500).json({error:err.message}) : res.status(201).json({id:this.lastID,heartRate,spO2});
    });
});
app.delete("/api/readings/:id", (req,res) => {
  db.run("DELETE FROM bd_health WHERE id=?",[req.params.id], function(err) {
    if(err) return res.status(500).json({error:err.message});
    if(!this.changes) return res.status(404).json({error:"Não encontrado"});
    res.json({deleted:req.params.id});
  });
});

// Quedas
app.get("/api/quedas", (req,res) => {
  const limit = Math.min(parseInt(req.query.limit)||50,200);
  db.all("SELECT * FROM bd_quedas ORDER BY timestamp DESC LIMIT ?",
    [limit], (err,rows) => err ? res.status(500).json({error:err.message}) : res.json(rows));
});
app.delete("/api/quedas/:id", (req,res) => {
  db.run("DELETE FROM bd_quedas WHERE id=?",[req.params.id], function(err) {
    if(err) return res.status(500).json({error:err.message});
    if(!this.changes) return res.status(404).json({error:"Não encontrado"});
    res.json({deleted:req.params.id});
  });
});

// Estatísticas
app.get("/api/stats", (req,res) => {
  db.get(`SELECT COUNT(*) AS total_leituras,
    ROUND(AVG(heartRate),1) AS media_bpm, ROUND(AVG(spO2),1) AS media_spo2,
    MAX(heartRate) AS max_bpm, MIN(spO2) AS min_spo2 FROM bd_health`,
    (err,h) => {
      if(err) return res.status(500).json({error:err.message});
      db.get("SELECT COUNT(*) AS total_quedas FROM bd_quedas", (e2,q) =>
        e2 ? res.status(500).json({error:e2.message}) : res.json({...h,...q}));
    });
});

app.listen(3001, () => {
  console.log("REST API em http://localhost:3001");
  console.log("  GET/POST /api/readings | DELETE /api/readings/:id");
  console.log("  GET      /api/quedas   | DELETE /api/quedas/:id");
  console.log("  GET      /api/stats");
  console.log("  Dashboard: http://localhost:3001/");
});

// ── Estado WoT ──────────────────────────────────────────────────────────────
const state = {
  heartRate:null, spO2:null, deviceReady:false,
  samplingIntervalMs:5000,
  thresholds:{heartRate:{min:0,max:100},spO2:{min:90}},
  vibrationActive:false, lastVibrationAt:null, vibrationTimer:null,
  fallDetectionActive:false, lastFallEvent:null, fallResetTimer:null,
  batteryLevel:0, connectionStatus:"offline",
  patient:{patientId:"patient001",displayName:"Paciente 001",age:65},
  _mqttConectado:false
};
let thingRef = null;

function avaliarLimiares() {
  if(!state.deviceReady) return [];
  const t=state.thresholds, alertas=[];
  if(state.heartRate!==null && state.heartRate>t.heartRate.max)
    alertas.push({source:"heartRate",value:state.heartRate,threshold:t.heartRate,
      severity:"red",patientId:state.patient.patientId,timestamp:new Date().toISOString()});
  if(state.spO2!==null && state.spO2<t.spO2.min)
    alertas.push({source:"spO2",value:state.spO2,threshold:t.spO2,
      severity:state.spO2<t.spO2.min-5?"red":"yellow",
      patientId:state.patient.patientId,timestamp:new Date().toISOString()});
  return alertas;
}

function processarQueda(payload) {
  let dados; try{dados=JSON.parse(payload);}catch{return;}
  const {tipo,intensidade,contagem}=dados;
  if(!tipo||!intensidade||typeof contagem!=="number") return;
  const agora=Date.now();
  if(agora-tsUltimaQuedaWoT<COOLDOWN_QUEDA_MS){
    console.log(`[Queda] Cooldown — ignorado (${Math.ceil((COOLDOWN_QUEDA_MS-(agora-tsUltimaQuedaWoT))/1000)}s)`);
    return;
  }
  tsUltimaQuedaWoT=agora;
  const evento={tipo,intensidade,contagem,patientId:state.patient.patientId,timestamp:new Date().toISOString()};
  state.fallDetectionActive=true; state.lastFallEvent=evento;
  guardarQueda(tipo,intensidade,contagem,state.patient.patientId);
  console.log(`[MQTT] ${tipo==="queda"?"QUEDA":"IMPACTO"} | ${intensidade} | contagem=${contagem}`);
  if(thingRef){
    thingRef.emitEvent("fallDetected",evento);
    if(!state.vibrationActive){
      const dur=tipo==="queda"?3000:1000, pat=tipo==="queda"?"sos":"long";
      state.vibrationActive=true; state.lastVibrationAt=new Date().toISOString();
      if(state.vibrationTimer) clearTimeout(state.vibrationTimer);
      state.vibrationTimer=setTimeout(()=>{
        state.vibrationActive=false;
        thingRef.emitEvent("vibrationCompleted",{durationMs:dur,pattern:pat,timestamp:new Date().toISOString()});
      },dur);
    }
  }
  if(state.fallResetTimer) clearTimeout(state.fallResetTimer);
  state.fallResetTimer=setTimeout(()=>{state.fallDetectionActive=false;},30000);
}

function iniciarMQTT(){
  console.log(`A ligar ao broker: ${MQTT_BROKER} ...`);
  const client=mqtt.connect(MQTT_BROKER,{clientId:MQTT_CLIENT_ID,clean:true,reconnectPeriod:5000});
  client.on("connect",()=>{
    state._mqttConectado=true;
    console.log("MQTT ligado ao test.mosquitto.org");
    client.subscribe([TOPIC_SENSOR,TOPIC_BEAT,TOPIC_QUEDA],{qos:1});
  });
  client.on("message",(topic,payload)=>{
    const msg=payload.toString();
    if(topic===TOPIC_BEAT){console.log(`[MQTT] Batimento: ${msg}`);return;}
    if(topic===TOPIC_QUEDA){processarQueda(msg);return;}
    if(topic===TOPIC_SENSOR){
      let d; try{d=JSON.parse(msg);}catch{return;}
      const hr=parseFloat(d.heartRate), spo2=parseFloat(d.spO2);
      if(isNaN(hr)||hr<30||hr>220||isNaN(spo2)||spo2<50||spo2>100){
        console.warn(`[MQTT] Ignorado: HR=${d.heartRate} SpO2=${d.spO2}`);return;}
      state.heartRate=Math.round(hr); state.spO2=parseFloat(spo2.toFixed(1));
      guardarLeitura(state.heartRate,state.spO2);
      if(!state.deviceReady){
        state.deviceReady=true; state.connectionStatus="online";
        console.log("\nESP32 ligado — deviceReady = true\n");
        if(thingRef) thingRef.emitEvent("deviceStatusChanged",
          {connectionStatus:"online",mqttConectado:true,timestamp:new Date().toISOString()});
      }
      console.log(`[MQTT] ESP32 → BPM: ${state.heartRate} | SpO2: ${state.spO2}%`);
      if(thingRef&&state.deviceReady){
        for(const a of avaliarLimiares()){
          console.log(`ALERTA ${a.severity.toUpperCase()} | ${a.source}=${a.value}`);
          thingRef.emitEvent("criticalHealthAlert",a);
          if(a.severity==="red"&&!state.vibrationActive){
            state.vibrationActive=true; state.lastVibrationAt=new Date().toISOString();
            if(state.vibrationTimer) clearTimeout(state.vibrationTimer);
            state.vibrationTimer=setTimeout(()=>{
              state.vibrationActive=false;
              thingRef.emitEvent("vibrationCompleted",{durationMs:3000,pattern:"sos",timestamp:new Date().toISOString()});
            },3000);
          }
        }
      }
    }
  });
  client.on("offline",()=>{
    state._mqttConectado=false; state.connectionStatus="offline"; state.deviceReady=false;
    if(thingRef) thingRef.emitEvent("deviceStatusChanged",
      {connectionStatus:"offline",mqttConectado:false,timestamp:new Date().toISOString()});
  });
  client.on("error",err=>console.error("Erro MQTT:",err.message));
}

// ── Arranque Servient WoT ───────────────────────────────────────────────────
const servient=new Servient();
servient.addServer(new HttpServer({port:8080}));

servient.start().then(async(WoT)=>{
  const thing=await WoT.produce({
    "@context":["https://www.w3.org/2022/wot/td/v1.1",
      {"healthiot":"https://w3id.org/iotschema/health#",
       "saref":"https://saref.etsi.org/core/",
       "om":"http://www.ontology-of-units-of-measure.org/resource/om-2/"}],
    "@type":["Thing","healthiot:WearableHealthMonitor","saref:Actuator"],
    id:"urn:dev:wot:health-monitor:wearable:patient001",
    title:"patient001",
    description:"ESP32 Wearable Health Monitor — node-wot + SQLite + REST API",
    version:{instance:"1.2.0"},
    securityDefinitions:{nosec_sc:{scheme:"nosec"}}, security:["nosec_sc"],
    properties:{
      heartRate:{type:"integer",readOnly:true,observable:true},
      spO2:{type:"number",readOnly:true,observable:true},
      deviceReady:{type:"boolean",readOnly:true,observable:true},
      fallDetectionActive:{type:"boolean",readOnly:true,observable:true},
      lastFallEvent:{type:"object",readOnly:true},
      vibrationActive:{type:"boolean",readOnly:true,observable:true},
      lastVibrationAt:{type:"string",readOnly:true},
      connectionStatus:{type:"string",readOnly:true,observable:true},
      batteryLevel:{type:"integer",readOnly:true},
      thresholds:{type:"object",readOnly:false,observable:true},
      samplingIntervalMs:{type:"integer",readOnly:false},
      patient:{type:"object",readOnly:true}
    },
    actions:{
      activateVibration:{title:"Activar vibração",synchronous:true,
        input:{type:"object",required:["duration_ms","pattern"],properties:{
          duration_ms:{type:"integer",minimum:100,maximum:10000},
          pattern:{type:"string",enum:["short","long","sos"]},
          intensity:{type:"integer",minimum:0,maximum:255}}},
        output:{type:"object",properties:{success:{type:"boolean"},startedAt:{type:"string"}}}},
      stopVibration:{title:"Parar vibração"},
      calibrate:{title:"Calibrar",
        input:{type:"object",properties:{mode:{type:"string",enum:["quick","full"]}}},
        output:{type:"object",properties:{success:{type:"boolean"},durationMs:{type:"integer"}}}},
      registerDevice:{title:"Registar no Directory",
        input:{type:"object",properties:{directoryUrl:{type:"string",format:"uri"}}}}
    },
    events:{
      criticalHealthAlert:{title:"Alerta Clínico",data:{type:"object",properties:{
        source:{type:"string"},value:{type:"number"},severity:{type:"string"},
        patientId:{type:"string"},timestamp:{type:"string"}}}},
      fallDetected:{title:"Queda detectada",data:{type:"object",properties:{
        tipo:{type:"string"},intensidade:{type:"string"},contagem:{type:"integer"},
        patientId:{type:"string"},timestamp:{type:"string"}}}},
      vibrationCompleted:{title:"Vibração concluída",data:{type:"object",properties:{
        durationMs:{type:"integer"},pattern:{type:"string"},timestamp:{type:"string"}}}},
      deviceStatusChanged:{title:"Estado dispositivo",data:{type:"object",properties:{
        connectionStatus:{type:"string"},mqttConectado:{type:"boolean"},timestamp:{type:"string"}}}}
    }
  });
  thingRef=thing;

  thing.setPropertyReadHandler("heartRate",async()=>state.heartRate??0);
  thing.setPropertyReadHandler("spO2",async()=>state.spO2??0);
  thing.setPropertyReadHandler("deviceReady",async()=>state.deviceReady);
  thing.setPropertyReadHandler("fallDetectionActive",async()=>state.fallDetectionActive);
  thing.setPropertyReadHandler("lastFallEvent",async()=>state.lastFallEvent??null);
  thing.setPropertyReadHandler("vibrationActive",async()=>state.vibrationActive);
  thing.setPropertyReadHandler("lastVibrationAt",async()=>state.lastVibrationAt);
  thing.setPropertyReadHandler("connectionStatus",async()=>state.connectionStatus);
  thing.setPropertyReadHandler("batteryLevel",async()=>0);
  thing.setPropertyReadHandler("thresholds",async()=>state.thresholds);
  thing.setPropertyReadHandler("samplingIntervalMs",async()=>state.samplingIntervalMs);
  thing.setPropertyReadHandler("patient",async()=>state.patient);

  thing.setPropertyWriteHandler("thresholds",async val=>{
    const v=await val.value();
    if(v.heartRate) state.thresholds.heartRate={...state.thresholds.heartRate,...v.heartRate};
    if(v.spO2) state.thresholds.spO2={...state.thresholds.spO2,...v.spO2};
    console.log("thresholds →",JSON.stringify(state.thresholds));
  });
  thing.setPropertyWriteHandler("samplingIntervalMs",async val=>{
    const v=await val.value();
    if(typeof v!=="number"||v<1000||v>60000) throw new Error(`Inválido: ${v}`);
    state.samplingIntervalMs=v;
  });

  thing.setActionHandler("activateVibration",async params=>{
    const{duration_ms,pattern="short"}=await params.value();
    if(!duration_ms||duration_ms<100||duration_ms>10000) throw new Error("duration_ms inválido");
    if(state.vibrationTimer) clearTimeout(state.vibrationTimer);
    state.vibrationActive=true; state.lastVibrationAt=new Date().toISOString();
    state.vibrationTimer=setTimeout(()=>{
      state.vibrationActive=false;
      thingRef.emitEvent("vibrationCompleted",{durationMs:duration_ms,pattern,timestamp:new Date().toISOString()});
    },duration_ms);
    return{success:true,startedAt:state.lastVibrationAt};
  });
  thing.setActionHandler("stopVibration",async()=>{
    if(state.vibrationTimer) clearTimeout(state.vibrationTimer);
    state.vibrationActive=false; return{success:true};
  });
  thing.setActionHandler("calibrate",async params=>{
    const input=params?await params.value():{};
    return{success:true,durationMs:input?.mode==="full"?5000:1200};
  });
  thing.setActionHandler("registerDevice",async params=>{
    const input=params?await params.value():{};
    console.log(`registerDevice → ${input?.directoryUrl}`);
    return{success:true,registeredAt:new Date().toISOString()};
  });

  iniciarMQTT();
  setInterval(()=>{
    if(thingRef) thingRef.emitEvent("deviceStatusChanged",
      {connectionStatus:state.connectionStatus,mqttConectado:state._mqttConectado,timestamp:new Date().toISOString()});
  },10000);

  await thing.expose();
  console.log("\nServient Producer + REST API + SQLite activos");
  console.log("  TD        → http://localhost:8080/patient001");
  console.log("  Dashboard → http://localhost:3001/\n");

}).catch(err=>{console.error("Erro Servient:",err);process.exit(1);});
