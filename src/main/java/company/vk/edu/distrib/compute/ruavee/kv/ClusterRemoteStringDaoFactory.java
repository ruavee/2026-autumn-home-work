package company.vk.edu.distrib.compute.ruavee.kv;

import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

@ClusterDaoFactoryTest
public class ClusterRemoteStringDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public ClusterRemoteStringDao create(int... ports) {
        return new ClusterRemoteStringDao(ports);
    }
}
