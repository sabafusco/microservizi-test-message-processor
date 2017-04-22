package com.example;

import com.example.processor.entity.Processo;
import com.example.processor.repository.IProcessoRepository;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
@SpringBootApplication
@ConfigurationProperties
public class MessageProcessorApplication  {

    private final static String QUEUE_NAME = "codaProcessi";
    private static String rabbitHost;
    private static int rabbitPort;
    private static String rabbitUser;
    private static String rabbitPass;  
  
    
   @Bean
    public CommandLineRunner process(IProcessoRepository repo) throws Exception {
        return (args) -> {  
        try{
            
                ConnectionFactory factory = new ConnectionFactory();
                
                factory.setHost(rabbitHost);
                factory.setUsername(rabbitUser);
                factory.setPassword(rabbitPass);
                factory.setPort(rabbitPort);
                
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                
                channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                
                Consumer consumer = new DefaultConsumer(channel) {
                  @Override
                  public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                      throws IOException {
                    String message = new String(body, "UTF-8");
                    if(message!=null){
                        Processo processo = readProcesso(repo, Long.valueOf(message));
                        if(processo!=null){
                            processo.setProcessato(1);
                            updateProcesso(repo,processo);
                        }
                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException ex) {
                            
                        }
                    }
                  }
                };
                channel.basicConsume(QUEUE_NAME, true, consumer);

        }catch(Exception e){
            e.printStackTrace();
            
        }
        };
    }
    
    private static Processo readProcesso(IProcessoRepository repo, long id) {
        Processo processo;
        try {
            processo = repo.findOne(id);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        if (processo == null) {
            String errorMst = "Nessun processo trovato con id " + id;
            return null;
        } else {
            return processo;
            //return processo.getId()+ ":" + processo.getProcessId()+":"+ processo.getTesto()+":"+ processo.getProcessato();
        }
    }

    private static Processo updateProcesso(IProcessoRepository repo ,Processo p) {
        Processo processo;
        try {
            processo = repo.findOne(p.getId());
            if(processo!=null){
                processo.setTesto(p.getTesto());
                processo.setProcessId(p.getProcessId());
                processo.setProcessato(p.getProcessato());
                repo.save(processo);
            }
           
        } catch (Exception e) {
            return null;
        }
        return processo;    
        //return processo.getId()+ ":" + processo.getProcessId()+":"+ processo.getTesto()+":"+ processo.getProcessato();
    }

    public static void main(String[] args) throws InterruptedException {
                        SpringApplication.run(MessageProcessorApplication.class, args);
    }

    @Value("${rabbit.host}")
    public void setRabbitHost(String rabbitHost) {
        MessageProcessorApplication.rabbitHost = rabbitHost;
    }

    @Value("${rabbit.port}")
    public void setRabbitPort(int rabbitPort) {
        MessageProcessorApplication.rabbitPort = rabbitPort;
    }

    @Value("${rabbit.user}")
    public void setRabbitUser(String rabbitUser) {
        MessageProcessorApplication.rabbitUser = rabbitUser;
    }

    @Value("${rabbit.pass}")
    public void setRabbitPass(String rabbitPass) {
        MessageProcessorApplication.rabbitPass = rabbitPass;
    }

    

}
