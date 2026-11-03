package company.vk.edu.distrib.compute.ruavee.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.KVServiceTest;
import company.vk.edu.distrib.compute.kv.ReplicatedKVServiceTest;

import java.io.IOException;

@KVServiceTest
@ReplicatedKVServiceTest
public class KVServiceFactory extends AbstractHttpServiceFactory<KVService> {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceImpl(port);
    }
}
