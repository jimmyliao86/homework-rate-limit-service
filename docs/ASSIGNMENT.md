<!--
  The original assignment brief, reproduced verbatim as handed out.
  The implementation's own README is at the repository root; the design and the
  reasoning behind it are in DESIGN.md in this directory.
-->

# 📬 Rate Limiting Service – Backend Homework

This is a technical assignment for backend engineer candidates. You are expected to build a RESTful rate-limiter service using **Spring Boot**, integrating **MySQL**, **Redis**, and **RocketMQ**.


---

## 🎯 Objective

Implement a simple API rate-limiting service that tracks usage by user or API key, and blocks requests exceeding the allowed threshold.

---

## 🔧 Features to Implement

### 1️⃣ Define Rate Limit

**Endpoint:** `POST /limits`

```json
{
  "apiKey": "abc-123",
  "limit": 100,
  "windowSeconds": 60
}
```

**Expected Behavior:**
- Set a request limit for a given API key within a time window
- Store this configuration in MySQL

---

### 2️⃣ Check API Access

**Endpoint:** `GET /check?apiKey=abc-123`

**Expected Behavior:**
- Increment usage counter for the key
- If usage exceeds the limit, return a blocked response
- Use Redis (INCR + EXPIRE) to track windowed usage

---

### 3️⃣ Query Usage

**Endpoint:** `GET /usage?apiKey=abc-123`

**Expected Behavior:**
- Return current usage count, remaining quota, and window TTL

---

### 4️⃣ Remove Limit Rule

**Endpoint:** `DELETE /limits/{apiKey}`

**Expected Behavior:**
- Remove the rate limit configuration from MySQL
- Clear any related Redis entries

---

### 5️⃣ View All Limits

**Endpoint:** `GET /limits`

**Expected Behavior:**
- List all active API keys and their associated limits
- Support pagination

---

🧪 Bonus (Optional)
- Use Spring Cache abstraction or RedisTemplate encapsulation
- Apply proper error handling with meaningful status codes
- Define your own DTO and message format for RocketMQ
- Use consistent and modular code structure (controller, service, repository, config, etc.)
- Test case coverage: as much as possible

⸻

🐳 Environment Setup

Use the provided docker-compose.yaml file to start required services:

Service	Port  
MySQL	3306  
Redis	6379  
RocketMQ Namesrv	9876  
RocketMQ Broker	10911  
RocketMQ Console	8088  

To start the services:

```commandline
docker-compose up -d
```

MySQL credentials:
- User: taskuser
- Password: taskpass
- Database: taskdb

You may edit init.sql to create required tables automatically.

⸻

🚀 Getting Started

To run the application:

./mvn spring-boot:run

Make sure to update your application.yml with the proper connections for:
- spring.datasource.url
- spring.redis.host
- rocketmq.name-server

⸻

📤 Submission

Please submit a `public Github repository` that includes:
- ✅ Complete and executable source code
- ✅ README.md (this file)
- ✅ Any necessary setup or data scripts please add them in HELP.md
- ✅ Optional: Postman collection or curl samples  

⸻

📌 Notes
- Focus on API correctness, basic error handling, and proper use of each technology
- You may use tools like Vibe Coding / ChatGPT to assist, but please write and understand your own code
- The expected time to complete is around 3 hours

Good luck!

---
