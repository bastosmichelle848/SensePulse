# SensePulse 📡

## 🫀 Intelligent Vital Signs Monitoring System using ESP32 and Web of Things (WoT)

SensePulse is an IoT-based embedded system for real-time monitoring of vital signs using an ESP32 microcontroller.  
It integrates the **Web of Things (WoT)** architecture through a **Thing Description (TD)**, enabling standardized communication between devices and applications.

---

## 🚀 Overview

The system collects physiological data from sensors connected to an ESP32 and exposes them through a WoT-compliant interface.  
It allows clients to access and consume vital signs data in a structured and interoperable way.

---

## 🧠 Features

- 📶 Real-time vital signs monitoring  
- 🔌 ESP32-based embedded system  
- 🌐 Web of Things (WoT) integration  
- 📄 Thing Description (TD) support  
- 🧩 Producer–Consumer architecture  
- 🔁 Node.js server implementation  
- 🏥 Healthcare IoT prototype  

---

## 🏗️ Architecture

- **ESP32 Device**
  - Collects sensor data (heart rate, etc.)
  - Sends data to the server

- **WoT Servient (Node.js)**
  - Implements Thing Description (TD)
  - Exposes data as a “Thing”
  - Handles communication between producer and consumer

- **Consumer Application**
  - Subscribes to data
  - Displays or processes vital signs

---

## 📄 Thing Description (TD)

This project uses JSON-based Thing Descriptions:

- `TD_Unified.json`
- `TD_Unified_bp.json`

They define:
- Properties
- Events
- Forms
- Protocol bindings

---

## 🧰 Technologies

- ESP32
- Node.js
- JavaScript
- Web of Things (WoT)
- JSON


---

## 📁 Project Structure
