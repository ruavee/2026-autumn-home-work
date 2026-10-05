package company.vk.edu.distrib.compute.ruavee.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

import java.io.IOException;

@RemoteDaoFactoryTest
public class RemoteStringDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) throws IOException {
        if (ports.length != 1) {
            throw new IllegalArgumentException("RemoteStringDaoFactory takes exactly one port");
        }
        int port = ports[0];
        KVService kvService = new KVServiceImpl(port);
        kvService.start();
        return new RemoteStringDao(port, kvService);
    }
}
