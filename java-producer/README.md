<h1> Producer - Consumer</h1>
This repository contains a Java application demonstrating the Producer-Consumer pattern using Apache Kafka.

<h2>Introduction</h2>

The Producer-Consumer pattern is a widely used design pattern in software engineering, particularly in distributed systems, to achieve asynchronous communication between components. In this demo, we utilize Apache Kafka as the messaging system to implement this pattern.

<h2>Overview</h2>

The application consists of two main components in the package config:

Producer: Responsible for generating data and sending it to Kafka topics.
Consumer: Consumes the data from Kafka topics and processes it accordingly.


<h2>Setup</h2>

Before running the application, ensure you have the following prerequisites:

Java Development Kit (JDK) installed.

Version 11

<h2>Usage</h2>

To run the application, follow these steps:

Clone this repository to your local machine.
<code>git clone "url of repo"</code> 
Navigate to the project directory.

<code>cd kafka-producer-conducer</code>

Execute the application.
<code> mvn spring-boot:run</code>
or directly on IDE Intelli J


<h2>Configuration Producer</h2>

The application's configuration is managed via Spring Boot configuration files. Below are the key configuration parameters:

<h4>ProducerConfig.BOOTSTRAP_SERVERS_CONFIG</h4>: This property specifies the Kafka servers the producer will connect to. In this case, the value is obtained from the bootstrapServers variable, which is retrieved from the configuration parameters.</h4>

<h4>ProducerConfig.ACKS_CONFIG</h4>: This property determines the acknowledgment level required to consider a message as successfully sent. The value acks_config is used for this, where it can be configured to all, meaning the leader and all replicas must have received the message before an acknowledgment is returned to the producer.

<h4>CommonClientConfigs.SECURITY_PROTOCOL_CONFIG</h4>: This property specifies the security protocol used to communicate with the Kafka server. In this case, the protocol is set to SASL_PLAINTEXT, indicating that communication should be secured with SASL (Simple Authentication and Security Layer) but without encryption.

<h4>SaslConfigs.SASL_MECHANISM</h4>: This property specifies the SASL mechanism used for authentication. Here, we use SCRAM-SHA-512, which is a salted hash-based mechanism, to secure authentication.</h4>

<h4>SaslConfigs.SASL_JAAS_CONFIG</h4>: This property defines the JAAS (Java Authentication and Authorization Service) configuration used for authentication with the Kafka server. In this example, we use ScramLoginModule and specify the username and password required for authentication.</h4>

You will find a kafka.conf file which contains the parameters for configuring the producer.
<code>src/main/resources/kafka.conf</code>



<code>

    public Map<String, Object> producerConfig(){
        Map<String,Object> props = new HashMap<>();
        // Spécification of server bootstrap
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG,acks_config);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.scram.ScramLoginModule required username='" + username + "' password='" + password + "';");
        return props;
    }
</code>


If your cluster has been configured with SASL configuration, please specify the security mechanisms such as the type of algorithm used(SCRAM-SHA-512), the security protocol(SASL_PLAINTEXT) and the credentials(SASL_JAAS_CONFIG).

please put the credentials in kafka.conf giving the username and password

username=<code>username</code>

password=<code>secret</code>


<h2>Kafka Message Sender</h2>
The kafkaMessageSender method within KafkaProducerConfig is responsible for reading JSON data from a file and sending it to the Kafka topic configured earlier. Each message is sent with a specified key.

<code>

    public CommandLineRunner kafkaMessageSender(KafkaTemplate<String, String> kafkaTemplate) {
        return args -> {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                JsonNode jsonNode = objectMapper.readTree(new File("src/main/resources/file.json"));
                jsonNode.forEach(messageNode -> {
                    String key = UUID.randomUUID().toString();
                    String message = messageNode.toString();
                    for (int i = 0; i < 10; i++) {
                        kafkaTemplate.send(topic, key, message);
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
    }
</code>

The method kafkaMessageSender is annotated with @Bean, which means it is configured as a Spring bean and will be executed at the application startup.

Inside this method, an ObjectMapper object is created to allow reading of the JSON file.

Then, the JSON file is read using objectMapper.readTree(new File("src/main/resources/file.json")), which returns a root node representing the entire JSON content.

A forEach loop is used to iterate over each message in the JSON file. At each iteration, a unique ID is generated using UUID.randomUUID().toString(), and the message is converted to a JSON string using messageNode.toString().

Finally, each message is sent 10 times (you can modify the loop for a high number of times) to Kafka using a for loop, where kafkaTemplate.send(topic, key, message) is called for each iteration with the Kafka topic, the generated key, and the JSON message.

