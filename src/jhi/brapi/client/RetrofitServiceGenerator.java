package jhi.brapi.client;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import fr.cirad.io.brapi.BrapiService;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import retrofit2.Converter;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

public class RetrofitServiceGenerator {
    private final String baseURL;
    private final String certificate;
    private OkHttpClient httpClient;
    private Retrofit retrofit;

    public RetrofitServiceGenerator(String baseURL, String certificate) {
        this.baseURL = baseURL;
        this.certificate = certificate;
    }

    public BrapiService generate(String authToken) {
        Interceptor inter = this.buildInterceptor(authToken);
        this.httpClient = new OkHttpClient.Builder().readTimeout(60L, TimeUnit.SECONDS).connectTimeout(60L, TimeUnit.SECONDS).addNetworkInterceptor(inter).build();
        try {
            this.httpClient = this.initCertificate(this.httpClient, this.certificate);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return this.buildService(this.baseURL, this.httpClient);
    }

    private Interceptor buildInterceptor(String authToken) {
        String bearer = "Bearer %s";
        Interceptor inter = chain -> {
            Request original = chain.request();
            if (original.header("Authorization") != null || authToken == null || authToken.isEmpty()) {
                return chain.proceed(original);
            }
            Request next = original.newBuilder().header("Authorization", String.format(bearer, authToken)).build();
            return chain.proceed(next);
        };
        return inter;
    }

    private OkHttpClient initCertificate(OkHttpClient client, String certificate) throws Exception {
        if (certificate == null || certificate.isEmpty()) {
            return client;
        }
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        InputStream caInput = new URL(certificate).openStream();
        Certificate ca = cf.generateCertificate(caInput);
        String keyStoreType = KeyStore.getDefaultType();
        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        keyStore.load(null, null);
        keyStore.setCertificateEntry("ca", ca);
        String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
        tmf.init(keyStore);
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, tmf.getTrustManagers(), null);
        client = client.newBuilder().sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager)tmf.getTrustManagers()[0]).hostnameVerifier((s, sslSession) -> true).build();
        caInput.close();
        return client;
    }

    private BrapiService buildService(String baseURL, OkHttpClient client) {
        this.retrofit = new Retrofit.Builder().baseUrl(baseURL).addConverterFactory((Converter.Factory)JacksonConverterFactory.create()).client(client).build();
        return (BrapiService) this.retrofit.create(BrapiService.class);
    }

    public InputStream getInputStream(URI uri) throws Exception {
        Request request = new Request.Builder().url(uri.toURL()).build();
        Response response = this.httpClient.newCall(request).execute();
        return response.body().byteStream();
    }

    Retrofit getRetrofit() {
        return this.retrofit;
    }
}

