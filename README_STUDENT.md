# Kafka

Projekt demonstruje komunikację między dwoma mikrousługami Spring Boot przez Apache Kafka:
* **order-service** (port 8081) – producent, przyjmuje zamówienie przez REST i wrzuca je do Kafki.
* **notification-serwice** (port 8082) – konsument, odbiera zamówienia i przetwarza je (powiadomienia, analityka).

Cała infrastruktura (3-węzłowy klaster Kafka w trybie KRaft + Kafka UI) wystartuje jednym poleceniem Docker Compose.

---

## 1. Co to jest Apache Kafka?

Kafka to rozproszona platforma do strumieniowania zdarzeń (event streaming). Działa jak bardzo szybka, trwała kolejka wiadomości, do której usługi mogą **pisać** i z której mogą **czytać** niezależnie od siebie.

Zamiast wywoływać inną usługę bezpośrednio (HTTP, gRPC), usługa A wrzuca zdarzenie do Kafki, a usługa B (lub wiele usług B) odbiera je we własnym tempie. Dzięki temu usługi są **odseparowane** (luźno powiązane) i nie muszą być dostępne w tym samym momencie.

---

## 2. Najważniejsze pojęcia

| Pojęcie | Co to jest |
|---------|------------|
| **Broker** | Pojedynczy serwer Kafki, który przechowuje partycje i obsługuje żądania producentów/konsumentów. |
| **Klaster** | Grupa brokerów współpracujących ze sobą (tutaj: 3 węzły `kafka1`, `kafka2`, `kafka3`). |
| **Topic** | Logiczny kanał / nazwa strumienia, np. `orders-topic`. Wszystkie wiadomości o zamówieniach trafiają pod ten temat. |
| **Partition** | Podział topicy na części fizyczne. Dzięki partiom Kafka może równolegle przetwarzać dane i skalować się. |
| **Offset** | Unikalny numer wiadomości w obrębie partycji. Konsument pamięta offset, żeby wiedzieć, do którego miejsca już czytał. |
| **Producer** | Aplikacja wysyłająca wiadomości do topicy (tutaj: `order-service`). |
| **Consumer** | Aplikacja odbierająca wiadomości z topicy (tutaj: `notification-serwice`). |
| **Consumer Group** | Grupa konsumentów dzielących się odczytami z topicy. Każda partycja trafia do dokładnie jednego konsumenta w grupie. Różne grupy mogą czytać te same wiadomości niezależnie. |
| **Key** | Klucz wiadomości (tutaj: `orderId`). Kafka gwarantuje, że wiadomości z tym samym kluczem trafią do tej samej partycji. |
| **Header** | Metadane dołączane do wiadomości (tutaj: `X-Correlation-Id`). |
| **KRaft** | Tryb pracy Kafki bez Zookeepara (wbudowane zarządzanie klastrem). Używamy go w `docker-compose.yml`. |

### Jak przepływają dane (schemat)

```
order-service                    Kafka (broker 1-3)             notification-serwice
   |                                 |                                |
   |---- POST /api/orders  --------->|  orders-topic                  |
   |      (producer, KafkaTemplate)  |  [partycja 0, 1, 2 ...]        |
   |                                 |<---- @KafkaListener -----------|
   |                                 |   groupId=notification-group   |
   |                                 |<---- @KafkaListener -----------|
   |                                 |   groupId=analytics-group      |
```

Ważne: obie grupy konsumentów (`notification-group` i `analytics-group`) czytają **te same** wiadomości z `orders-topic`, ale każda we własnym tempie i z własnym offsetem.

---

## 3. Struktura projektu

```
kafka-example/
├── docker-compose.yml              # 3x Kafka KRaft + Kafka UI (port 9090)
├── order-service/                  # Producent (port 8081)
│   ├── src/main/java/org/example/orderservice/
│   │   ├── OrderEvent.java         # Model zdarzenia zamówienia
│   │   ├── OrderController.java    # REST + KafkaTemplate (wysyłka do Kafki)
│   │   └── OrderServiceApplication.java
│   └── src/main/resources/application.properties
├── notification-serwice/           # Konsument (port 8082)
│   ├── src/main/java/org/example/notificationservice/
│   │   ├── OrderEvent.java         # Model (deserializacja JSON z Kafki)
│   │   ├── NotificationListener.java   # Konsument powiadomień (grupa notification-group)
│   │   ├── AnalyticsListener.java      # Konsument analityki (grupa analytics-group)
│   │   ├── KafkaConsumerConfig.java    # Obsługa błędów + Dead Letter Queue
│   │   └── NotificationServiceApplication.java
│   └── src/main/resources/application.properties
└── README_STUDENT.md
```

---

## 4. Uruchomienie projektu

### Wymagania
* Docker + Docker Compose
* Java 21+ (Gradle toolchain pobierze odpowiedni JDK)
* Gradle Wrapper jest już dołączony (`gradlew`)

### Krok 1 – Uruchom Kafka i Kafka UI

```bash
docker-compose up -d
```

Kafka UI będzie dostępne pod: http://localhost:9090  
Broker Kafka na: `localhost:19092,localhost:29092,localhost:39092`

### Krok 2 – Uruchom order-service

```bash
cd order-service
./gradlew bootRun
```

### Krok 3 – Uruchom notification-serwice

(w nowym terminalu)

```bash
cd notification-serwice
./gradlew bootRun
```

### Krok 4 – Wyślij testowe zamówienie

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"productName":"Laptop","price":2999.99}'
```

W konsoli `notification-serwice` powinny pojawić się logi z obu konsumentów.

### Krok 5 – Monitoring: Grafana i Prometheus

Po uruchomieniu wszystkich kontenerów i obu mikroserwisów monitoring jest dostępny pod adresami:

* **Grafana:** http://localhost:3000
  * login: `admin`
  * hasło: `admin`
  * dashboard **Kafka Example - mikroserwisy i Kafka** jest załadowany automatycznie.
* **Prometheus:** http://localhost:9091
  * zapytania można sprawdzać w zakładce **Graph**.

Prometheus pobiera metryki mikroserwisów z endpointów:

* http://localhost:8081/actuator/prometheus
* http://localhost:8082/actuator/prometheus

Eksporter Kafki udostępnia metryki pod adresem http://localhost:9308/metrics.

#### Wysłanie pięciu zamówień

Poniższa pętla wysyła od razu pięć niezależnych żądań do `order-service`:

```bash
for i in {1..5}; do
  curl -s -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"productName\":\"Laptop-$i\",\"price\":$((1000 + i * 100))}"
  echo
done
```

#### Podstawowe zapytania PromQL

Zapytania można wkleić w Prometheusie (`http://localhost:9091`) albo użyć ich jako wyrażeń paneli w Grafanie:


#### Czy mikroserwisy i eksporter Kafki są dostępne? 1 = UP, 0 = DOWN
```promql
up
```

#### Stan obu mikroserwisów
```promql
up{job=~"order-service|notification-service"}
```

#### Liczba żądań HTTP - wszystkie żądania:
```promql
http_server_requests_seconds_count
```

#### Liczba brokerów Kafki
```promql
kafka_brokers
```

#### Liczba wiadomości we wszystkich topicach (bez topiców systemowych)
```promql
sum(
  kafka_topic_partition_current_offset{topic!~"__.*"}
  - kafka_topic_partition_oldest_offset{topic!~"__.*"}
)
```

#### Łączny consumer lag według grupy konsumentów
```promql
sum by (consumergroup) (kafka_consumergroup_lag_sum)
```

#### Liczba wywołań endpointu zamówień:
```promql
sum(
    http_server_requests_seconds_count{
    uri="/api/orders"
    }
)
```
#### Metryki na dashboardzie Grafany

| Panel | Co pokazuje |
|---|---|
| Stan mikroserwisów | Czy `order-service` i `notification-service` odpowiadają na scrape Prometheusa (`UP` / `DOWN`). |
| Dostępność eksportera Kafka | Czy `kafka-exporter` jest dostępny i dostarcza metryki klastra Kafka. |
| Liczba brokerów | Liczbę brokerów wykrytych w klastrze Kafka. |
| Żądania HTTP na sekundę | Tempo obsługi żądań HTTP przez mikroserwisy, rozdzielone według aplikacji i URI. |
| Czas odpowiedzi HTTP (p95) | Czas odpowiedzi, poniżej którego mieści się 95% żądań; wzrost wartości oznacza wolniejsze odpowiedzi. |
| Liczba partycji  | Łączną liczbę partycji topiców Kafka. |
| Wiadomości w topicach  | Przybliżoną liczbę dostępnych wiadomości w topicach, z pominięciem topiców systemowych. |
| Pamięć JVM | Zużycie pamięci heap JVM przez każdy mikroserwis. |
| Consumer lag | Liczbę wiadomości oczekujących na odczyt przez każdą grupę konsumentów; wzrost może oznaczać opóźnienie konsumenta. |
| Replikacja i ISR | Liczbę replik partycji oraz replik aktualnie zsynchronizowanych (ISR). |

---

## 5. Kod do wklejania (live coding)

Poniżej znajdują się fragmenty kodu w kolejności, w jakiej warto je wpisać podczas laboratorium. Możesz je skopiować i wkleić do odpowiednich plików.

### Krok 0 – Model zdarzenia (OrderEvent)

Utwórz plik `OrderEvent.java` w obu serwisach (w swoich pakietach):

**order-service:** `src/main/java/org/example/orderservice/OrderEvent.java`
**notification-serwice:** `src/main/java/org/example/notificationservice/OrderEvent.java`

```java
public class OrderEvent {
    private String orderId;
    private String productName;
    private Double price;
    private Integer schemaVersion = 1;

    public OrderEvent() {}

    public OrderEvent(String orderId, String productName, Double price) {
        this.orderId = orderId;
        this.productName = productName;
        this.price = price;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public String getProductName() { return productName; }
    public void setProductName(String productName) { this.productName = productName; }
    public Double getPrice() { return price; }
    public void setPrice(Double price) { this.price = price; }
    public Integer getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(Integer schemaVersion) { this.schemaVersion = schemaVersion; }
}
```

> **Uwaga:** Bezparametrowy konstruktor i gettery/settery są konieczne, aby Spring mógł (des)serializować obiekt do/ z JSON.

---

### Krok 1 – Producent: wysyłka wiadomości do Kafki (`OrderController`)

Plik: `order-service/src/main/java/org/example/orderservice/OrderController.java`

```java
package org.example.orderservice;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderController(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public ResponseEntity<?> placeOrder(@RequestBody OrderEvent orderEvent) {
        if (orderEvent.getOrderId() == null) {
            orderEvent.setOrderId(UUID.randomUUID().toString());
        }

        String correlationId = UUID.randomUUID().toString();
        ProducerRecord<String, OrderEvent> record =
                new ProducerRecord<>("orders-topic", orderEvent.getOrderId(), orderEvent);
        record.headers().add("X-Correlation-Id", correlationId.getBytes());

        kafkaTemplate.send(record);

        return ResponseEntity.ok()
                .body("Zamówienie " + orderEvent.getOrderId() + " przekazane do Kafki!");
    }
}
```

W `order-service/src/main/resources/application.properties` upewnij się, że masz:

```properties
server.port=8081

spring.kafka.bootstrap-servers=localhost:19092,localhost:29092,localhost:39092

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
spring.kafka.producer.acks=all
spring.kafka.producer.retries=3
spring.kafka.producer.properties.spring.json.add.type.headers=false
```

---

### Krok 1b – DTO i transakcje (rozbudowa producenta)

Zamiast przesyłać domenową klasę `OrderEvent` bezpośrednio przez Kafkę, lepiej użyć **DTO (Data Transfer Object)**. DTO rozdziela to, co wysyłasz na zewnątrz (Kafka), od tego, co masz wewnątrz aplikacji.

#### Klasa Mapper (OrderMapper)

Mapper konwertuje między DTO (do Kafki) a domenową encją `OrderEvent` (wewnątrz aplikacji). Dzięki temu warstwa Kafka nie zna modelu domeny, a domena nie wie o Kafce.

Utwórz plik `OrderMapper.java` w obu serwisach:

**order-service:** `src/main/java/org/example/orderservice/OrderMapper.java`
**notification-serwice:** `src/main/java/org/example/notificationservice/OrderMapper.java`

```java
package org.example.orderservice; // lub org.example.notificationservice

import org.springframework.stereotype.Component;

@Component
public class OrderMapper {

    public OrderEvent toDomain(OrderDto dto) {
        OrderEvent event = new OrderEvent();
        event.setOrderId(dto.getOrderId());
        event.setProductName(dto.getProductName());
        event.setPrice(dto.getPrice());
        event.setSchemaVersion(1);
        return event;
    }

    public OrderDto toDto(OrderEvent event) {
        OrderDto dto = new OrderDto();
        dto.setOrderId(event.getOrderId());
        dto.setProductName(event.getProductName());
        dto.setPrice(event.getPrice());
        return dto;
    }
}
```

#### Zmieniony `OrderController` z transakcją

Wysyłka w transakcji gwarantuje, że wiadomość trafi do Kafki tylko jeśli cała operacja się powiedzie. Jeśli wystąpi błąd, transakcja zostanie cofnięta.

```java
package org.example.orderservice;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final KafkaTemplate<String, OrderDto> kafkaTemplate;
    private final OrderMapper orderMapper;
    private final KafkaTransactionManager<String, OrderDto> transactionManager;

    public OrderController(KafkaTemplate<String, OrderDto> kafkaTemplate,
                           OrderMapper orderMapper,
                           KafkaTransactionManager<String, OrderDto> transactionManager) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderMapper = orderMapper;
        this.transactionManager = transactionManager;
    }

    @PostMapping
    @Transactional
    public ResponseEntity<?> placeOrder(@RequestBody OrderDto orderDto) {
        if (orderDto.getOrderId() == null) {
            orderDto.setOrderId(UUID.randomUUID().toString());
        }

        OrderEvent orderEvent = orderMapper.toDomain(orderDto);

        String correlationId = UUID.randomUUID().toString();
        ProducerRecord<String, OrderDto> record =
                new ProducerRecord<>("orders-topic", orderDto.getOrderId(), orderDto);
        record.headers().add("X-Correlation-Id", correlationId.getBytes());

        System.out.println("Otrzymano żądanie HTTP. Przetwarzanie zamówienia: " + orderEvent.getProductName());
        kafkaTemplate.send(record);

        return ResponseEntity.ok()
                .body("Zamówienie " + orderDto.getOrderId() + " przekazane do Kafki!");
    }
}
```

#### Konfiguracja menedżera transakcji Kafka

Dodaj bean `KafkaTransactionManager` w order-service (nowy plik `KafkaConfig.java`):

```java
package org.example.orderservice;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTransactionManager;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaConfig {

    @Bean
    public KafkaTransactionManager<String, OrderDto> kafkaTransactionManager(
            ProducerFactory<String, OrderDto> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }
}
```

W `application.properties` producenta upewnij się, że masz też:

```properties
spring.kafka.producer.transaction-id-prefix=order-tx-
spring.kafka.producer.properties.enable.idempotence=true
```

**Co warto omówić na żywo:**
* **DTO** – klasa tylko do przesyłania danych, bez logiki biznesowej. Dzięki niej zmiana w domenie nie wymusza zmiany formatu wiadomości w Kafce.
* **Mapper** – konwertuje między DTO a domeną. Rozdziela warstwę transportu (Kafka) od logiki biznesowej.
* **`@Transactional`** + `KafkaTransactionManager` – wysyłka do Kafki w transakcji. Albo wszystko się uda, albo nic (rollback).
* **Idempotencja** (`enable.idempotence=true`) – Kafka nie dostarczy tego samego komunikatu dwa razy, nawet jeśli producent ponownie wyśle po awarii sieci.

---

### Krok 2 – Konsument: odbiór wiadomości (`NotificationListener`)

Plik: `notification-serwice/src/main/java/org/example/notificationservice/NotificationListener.java`

```java
package org.example.notificationservice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class NotificationListener {

    private final OrderMapper orderMapper;

    public NotificationListener(OrderMapper orderMapper) {
        this.orderMapper = orderMapper;
    }

    @KafkaListener(topics = "orders-topic", groupId = "notification-group")
    public void consumeOrder(ConsumerRecord<String, OrderDto> record, Acknowledgment ack) {
        OrderDto dto = record.value();
        OrderEvent orderEvent = orderMapper.toDomain(dto);

        System.out.println("-------------------------------------------------");
        System.out.println("Odebrano wiadomość z Kafki!");
        System.out.println("Temat: " + record.topic());
        System.out.println("Partycja: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Klucz: " + record.key());
        System.out.println("Wysyłam email potwierdzający dla klienta...");
        System.out.println("Zamówiono produkt: " + orderEvent.getProductName());
        System.out.println("Kwota: " + orderEvent.getPrice() + " PLN");
        System.out.println("-------------------------------------------------");

        ack.acknowledge();
    }
}
```

W `notification-serwice/src/main/resources/application.properties` upewnij się, że masz:

```properties
server.port=8082

spring.kafka.bootstrap-servers=localhost:19092,localhost:29092,localhost:39092

spring.kafka.consumer.group-id=notification-group
spring.kafka.consumer.auto-offset-reset=earliest

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.value.default.type=org.example.notificationservice.OrderDto
spring.kafka.consumer.properties.spring.json.trusted.packages=org.example.notificationservice,org.example.orderservice
spring.kafka.listener.ack-mode=manual_immediate
spring.kafka.listener.concurrency=3
```

---

### Krok 3 – Drugi konsument w innej grupie (`AnalyticsListener`)

Plik: `notification-serwice/src/main/java/org/example/notificationservice/AnalyticsListener.java`

```java
package org.example.notificationservice;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AnalyticsListener {

    private final OrderMapper orderMapper;

    public AnalyticsListener(OrderMapper orderMapper) {
        this.orderMapper = orderMapper;
    }

    @KafkaListener(topics = "orders-topic", groupId = "analytics-group")
    public void consumeOrder(OrderDto orderDto) {
        OrderEvent orderEvent = orderMapper.toDomain(orderDto);
        System.out.println("[analytics] Zamówienie: " + orderEvent.getProductName()
                + " kwota=" + orderEvent.getPrice());
    }
}
```

---

### Krok 4 – Obsługa błędów i Dead Letter Queue (`KafkaConsumerConfig`)

Plik: `notification-serwice/src/main/java/org/example/notificationservice/KafkaConsumerConfig.java`

```java
package org.example.notificationservice;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(kafkaTemplate);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
    }
}
```

---

## 6. Ściąga – komendy i endpointy

Uruchomienie infrastruktury:
```bash
docker-compose up -d
```

Wyślij zamówienie (test producenta):
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"productName":"Książka","price":49.99}'
```

Kafka UI (podgląd tematów, partycji, offsetów, DLT):
```
http://localhost:9090
```

Kluczowe adnotacje i klasy Spring Kafka:
* `@KafkaListener` – subskrypcja topicy
* `KafkaTemplate` – wysyłka wiadomości
* `ProducerRecord` – wiadomość z tematem, kluczem, wartością i nagłówkami
* `ConsumerRecord` – odebrana wiadomość z metadanymi
* `Acknowledgment` – ręczne potwierdzenie przetworzenia
