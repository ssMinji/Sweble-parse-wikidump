package step02.join;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.core.Index;

public class JestTest {
	JestClientFactory factory = new JestClientFactory();
    factory.setHttpClientConfig(new HttpClientConfig
              .Builder("http://elastic.pslicore.io:9200")
              .multiThreaded(true)
              .build());
    JestClient client = factory.getObject();
    Index index = new Index.Builder(jsonHtml).index("pagerank").type("page").build();
}

