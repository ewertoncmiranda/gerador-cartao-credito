package miranda.app.geradorcartaocredito;

import io.quarkus.scheduler.Scheduled;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Singleton;
import java.util.List;

@ApplicationScoped
public class ListenerPropostaQueue {

    @ConfigProperty(name="app.config.message.queue.topic")
    private String endpoint;

    @Scheduled(every="10s")
    public void receiveMessages() {

        System.out.println("\nReceive messages");
        try {

            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                        .queueUrl(endpoint)
                        .maxNumberOfMessages(5)
                    .build();

            List<Message> messages = sqsClient().receiveMessage(receiveMessageRequest).messages();
            messages.stream().forEach(it->{
                System.out.println(it.body());
            });


        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

    }

    public SqsClient sqsClient(){
        return SqsClient.builder()
                .region(Region.SA_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
    }
}
