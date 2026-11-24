package company.vk.edu.distrib.compute.ruavee.moderation;

import company.vk.edu.distrib.compute.moderation.ModerationDeadLetterTest;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactory;
import company.vk.edu.distrib.compute.moderation.ModerationServiceFactoryTest;

@ModerationServiceFactoryTest
@ModerationDeadLetterTest
public final class RuaveeModerationServiceFactory implements ModerationServiceFactory {
    @Override
    public ModerationService create(String bootstrapServers) {
        return new ModerationServiceImpl(bootstrapServers);
    }
}
