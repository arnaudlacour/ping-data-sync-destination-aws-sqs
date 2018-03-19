package au.com.versent.au.com.versent.ping.data.sync.destination.aws;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.unboundid.directory.sdk.common.types.LogSeverity;
import com.unboundid.directory.sdk.sync.api.SyncDestination;
import com.unboundid.directory.sdk.sync.config.SyncDestinationConfig;
import com.unboundid.directory.sdk.sync.types.EndpointException;
import com.unboundid.directory.sdk.sync.types.PostStepResult;
import com.unboundid.directory.sdk.sync.types.SyncOperation;
import com.unboundid.directory.sdk.sync.types.SyncServerContext;
import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.Modification;
import com.unboundid.util.args.ArgumentException;
import com.unboundid.util.args.ArgumentParser;
import com.unboundid.util.args.StringArgument;
import com.unboundid.util.json.JSONBuffer;

import java.util.List;

/**
 * This class implement logic to allow pushing changes to AWS SQS
 *
 *
 */
public class SQS extends SyncDestination
{
    private SyncServerContext serverContext;
    private String queueUrl;
    private AmazonSQS sqsClient;
    
    /**
     * Performs the necessary processing to produce an extension name
     * @return a string with the extension name
     */
    @Override
    public String getExtensionName()
    {
        return "Amazon SQS Sync Destination";
    }
    
    /**
     * Performs the necessary processing to produce an array of strings with a detailed description of the extension
     * purpose and how it works from the user's or administrator's perspective. Each string appears as a separate
     * paragrapg
     * @return an array of strings with description about the extension
     */
    @Override
    public String[] getExtensionDescription()
    {
        return new String[] {"This is a simple extension that publishes JSON to Amazon Web Services Simple Queue Service"};
    }
    
    /**
     * Performs the necessary processing to generate an endpoint URL that may be used to distinguish different instances
     * of this extension interacting with difference SQS queues
     * @return a string representation of an endpoint URL
     */
    @Override
    public String getCurrentEndpointURL()
    {
        return null;
    }
    
    /**
     * This method is used to define configuration argument that may be used to adjust the behavior of an instance of
     * the extension.
     * @param parser the argument parser that is used to declare and later process arguments provided via dsconfig, the WE console or the configuration API
     * @throws ArgumentException if any of the arguments could not be defined
     */
    @Override
    public void defineConfigArguments(ArgumentParser parser) throws ArgumentException
    {
        parser.addArgument(new StringArgument(null, "queue",true,1,"{queue name}","The name of the SQS queue to publish changes to."));
    }
    
    /**
     * Performs the necessary processing to gracefull shutdown the instance of the extension
     */
    @Override
    public void finalizeSyncDestination()
    {
        sqsClient.shutdown();
    }
    
    /**
     * Performs the necessary processing to initialize the instance of the extension such that it is ready to
     * process traffic. This is only called when the extension is initially enabled.
     * @param serverContext the Sync server context
     * @param config the configuration object of the instance of the extension
     * @param parser the argument parser
     * @throws EndpointException in case initialization could not be properly
     */
    @Override
    public void initializeSyncDestination(SyncServerContext serverContext, SyncDestinationConfig config, ArgumentParser parser) throws EndpointException
    {
        this.serverContext = serverContext;
        String queueName = parser.getStringArgument("queue").getValue();
        // we blissfully omit authentication here, which should work in most environment with IAM
        sqsClient = AmazonSQSClientBuilder.standard().build();
        try
        {
            GetQueueUrlResult getQueueUrlResult = sqsClient.getQueueUrl(queueName);
            queueUrl = getQueueUrlResult.getQueueUrl();
        } catch ( QueueDoesNotExistException qdnee )
        {
            String message = "Not much we can do, this queue ("+queueName+") does not exist.";
            serverContext.logMessage(LogSeverity.FATAL_ERROR, message);
            throw new EndpointException(PostStepResult.ABORT_OPERATION, message, qdnee);
        }
    }
    
    @Override
    public void createEntry(Entry entry, SyncOperation syncOperation) throws EndpointException
    {
        sqsClient.sendMessage(queueUrl,entryToJSONBuffer(entry).toString());
    }
    
    @Override
    public void modifyEntry(Entry entry, List<Modification> list, SyncOperation syncOperation) throws EndpointException
    {
        sqsClient.sendMessage(queueUrl,entryToJSONBuffer(entry).toString());
    }
    
    @Override
    public void deleteEntry(Entry entry, SyncOperation syncOperation) throws EndpointException
    {
        sqsClient.sendMessage(queueUrl,entryToJSONBuffer(entry).toString());
    }
    
    /**
     * Performs minimal processing to convert an LDAP Attribute to valid JSON
     * @param entry the LDAP entry to append to the JSON buffer
     */
    private JSONBuffer entryToJSONBuffer(Entry entry)
    {
        if ( entry == null )
        {
            return null;
        }
        
        JSONBuffer jsonBuffer = new JSONBuffer();
        jsonBuffer.beginObject();
        jsonBuffer.appendString("dn",entry.getDN());
        for (Attribute attribute: entry.getAttributes()){
            processAttribute(jsonBuffer,attribute);
        }
        jsonBuffer.endObject();
        return jsonBuffer;
    }
    
    /**
     * Performs minimal processing to convert an LDAP Attribute to valid JSON
     * @param buffer the JSON buffer to append content to
     * @param attribute the LDAP attribute to produce content from
     */
    private void processAttribute(JSONBuffer buffer, Attribute attribute)
    {
        if ( attribute != null && buffer != null )
        {
            if (attribute.getValues().length > 1)
            {
                buffer.beginArray(attribute.getBaseName());
                for (String value : attribute.getValues())
                {
                    buffer.appendString(value);
                }
                buffer.endArray();
            } else
            {
                buffer.appendString(attribute.getBaseName(), attribute.getValue());
            }
        }
    }
}