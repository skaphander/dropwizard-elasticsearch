package io.dropwizard.elasticsearch.managed;

import com.google.common.collect.ImmutableList;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;

import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.Iterator;

import javax.net.ssl.SSLContext;

import io.dropwizard.elasticsearch.config.EsConfiguration;
import io.dropwizard.lifecycle.Managed;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * A Dropwizard managed Elasticsearch {@link RestHighLevelClient}.
 * Depending on the {@link EsConfiguration} a High Level Rest Client
 * a {@link RestHighLevelClient} a is being created and its lifecycle is managed by Dropwizard.
 */
public class ManagedEsClient implements Managed {
    private RestHighLevelClient client = null;
    private Sniffer sniffer = null;

    public ManagedEsClient(final EsConfiguration config) throws Exception {
        checkNotNull(config, "EsConfiguration must not be null");

        RestClientBuilder restClientBuilder = RestClient.builder(config.getServersAsHttpHosts().toArray(new HttpHost[0]));
        setRequest(restClientBuilder, config);
        final ImmutableList.Builder<RestClientBuilder.HttpClientConfigCallback> configCallbackBuilder = ImmutableList.builder();
        if (!config.getHeadersAsHeaders().isEmpty()) {
            restClientBuilder.setDefaultHeaders(config.getHeadersAsHeaders().toArray(new Header[0]));
        }

        if (config.getNumberOfThreads() > 0) {
            configCallbackBuilder.add(createThreadsConfigCallback(config));
        }

        if (config.getNode() != null && !config.getNode().isEmpty()) {
            setNodeSelector(restClientBuilder, config);
        }

        if (config.getBasicAuthentication() != null) {
            configCallbackBuilder.add(createCredentialConfigCallback(config));
        }

        if (config.getKeystore() != null) {
            configCallbackBuilder.add(createKeystoreConfigCallback(config));
        }
        configureHttpClient(restClientBuilder, configCallbackBuilder);
        if (config.getSniffer() != null) {
            if (config.getSniffer().getSniffOnFailure()) {
                SniffOnFailureListener sniffOnFailureListener =
                        new SniffOnFailureListener();
                restClientBuilder.setFailureListener(sniffOnFailureListener);
                this.client = new RestHighLevelClient(restClientBuilder);
                this.sniffer = Sniffer.builder(this.client.getLowLevelClient())
                        .setSniffAfterFailureDelayMillis(config.getSniffer().getSniffAfterFailureDelayMillis())
                        .build();
                sniffOnFailureListener.setSniffer(this.sniffer);

            } else {
                this.client = new RestHighLevelClient(restClientBuilder);
                this.sniffer = Sniffer.builder(this.client.getLowLevelClient())
                        .setSniffIntervalMillis(config.getSniffer().getSniffIntervalMillis())
                        .build();
            }

        } else {
            this.client = new RestHighLevelClient(restClientBuilder);
        }
    }

    private void configureHttpClient(RestClientBuilder restClientBuilder, ImmutableList.Builder<RestClientBuilder.HttpClientConfigCallback> configCallbackBuilder) {
        ImmutableList<RestClientBuilder.HttpClientConfigCallback> configCallbacks = configCallbackBuilder.build();
        if (!configCallbacks.isEmpty()) {
            restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
                HttpAsyncClientBuilder builder = httpClientBuilder;
                for (RestClientBuilder.HttpClientConfigCallback callback : configCallbacks) {
                    builder = callback.customizeHttpClient(httpClientBuilder);
                }
                return builder;
            });
        }
    }

    /**
     * Create a new managed Elasticsearch {@link Client} from the provided {@link Client}.
     *
     * @param client an initialized {@link Client} instance
     */
    public ManagedEsClient(RestHighLevelClient client) {
        this.client = checkNotNull(client, "Elasticsearch client must not be null");
    }

    /**
     * Create a new managed Elasticsearch {@link Client} from the provided {@link Client}.
     *
     * @param client an initialized {@link Client} instance
     */
    public ManagedEsClient(RestHighLevelClient client, Sniffer sniffer) {
        this.client = checkNotNull(client, "Elasticsearch client must not be null");
        this.sniffer = checkNotNull(sniffer, "Sniffer must not be null");
    }

    /**
     * Starts the Elasticsearch {@link Node} (if appropriate). Called <i>before</i> the service becomes available.
     *
     * @throws Exception if something goes wrong; this will halt the service startup.
     */
    @Override
    public void start() throws Exception {
    }

    /**
     * Stops the Elasticsearch {@link Client} and (if appropriate) {@link Node} objects. Called <i>after</i> the service
     * is no longer accepting requests.
     *
     * @throws Exception if something goes wrong.
     */
    @Override
    public void stop() throws Exception {
        closeClient();
    }

    /**
     * Get the managed Elasticsearch {@link Client} instance.
     *
     * @return a valid Elasticsearch {@link Client} instance
     */
    public RestHighLevelClient getClient() {
        return client;
    }


    private void closeClient() throws Exception {
        if (null != client) {
            client.close();
        }
        if (null != sniffer) {
            sniffer.close();
        }
    }

    private void setRequest(RestClientBuilder restClientBuilder, EsConfiguration config) {
        restClientBuilder.setRequestConfigCallback(
                requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(config.getConnectTimeOut())
                        .setSocketTimeout(config.getSocketTimeOut()));
    }

    private RestClientBuilder.HttpClientConfigCallback createThreadsConfigCallback(EsConfiguration config) {
        return httpClientBuilder -> httpClientBuilder.setDefaultIOReactorConfig(
                IOReactorConfig.custom()
                        .setIoThreadCount(config.getNumberOfThreads())
                        .build());
    }

    private RestClientBuilder.HttpClientConfigCallback createCredentialConfigCallback(EsConfiguration config) {
        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(config.getBasicAuthentication().getUser(), config.getBasicAuthentication().getPassword()));
        return httpClientBuilder -> httpClientBuilder
                .setDefaultCredentialsProvider(credentialsProvider);
    }

    private RestClientBuilder.HttpClientConfigCallback createKeystoreConfigCallback(EsConfiguration config) throws Exception {
        KeyStore truststore = KeyStore.getInstance(config.getKeystore().getType());
        try (InputStream is = Files.newInputStream(config.getKeystore().getKeyStorePath())) {
            truststore.load(is, config.getKeystore().getKeyStorePass().toCharArray());
        }
        SSLContextBuilder sslBuilder = SSLContexts.custom()
                .loadTrustMaterial(truststore, null);
        final SSLContext sslContext = sslBuilder.build();
        return httpClientBuilder -> httpClientBuilder.setSSLContext(sslContext);
    }

    private void setNodeSelector(RestClientBuilder restClientBuilder, EsConfiguration config) {
        restClientBuilder.setNodeSelector(new NodeSelector() {
            @Override
            public void select(Iterable<Node> nodes) {
                /*
                 * Prefer any node that belongs to rack_one. If none is around
                 * we will go to another rack till it's time to try and revive
                 * some of the nodes that belong to rack_one.
                 */
                boolean foundOne = false;
                for (Node node : nodes) {
                    String rackId = node.getAttributes().get("rack_id").get(0);
                    if (config.getNode().equals(rackId)) {
                        foundOne = true;
                        break;
                    }
                }
                if (foundOne) {
                    Iterator<Node> nodesIt = nodes.iterator();
                    while (nodesIt.hasNext()) {
                        Node node = nodesIt.next();
                        String rackId = node.getAttributes().get("rack_id").get(0);
                        if (config.getNode().equals(rackId) == false) {
                            nodesIt.remove();
                        }
                    }
                }
            }
        });
    }
}
