package alpakka.sample.sqssample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.AbstractActor;

class EnrichActor extends AbstractActor {

    final static Logger log = LoggerFactory.getLogger(EnrichActor.class);

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Integer.class, message -> {
                    log.debug("actor received '{}'", message);
                    sender().tell(new ActorResponseMsg(message.toString()), self());
                })
                .build();
    }
}
