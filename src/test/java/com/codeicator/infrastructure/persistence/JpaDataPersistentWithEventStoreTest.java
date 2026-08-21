package com.codeicator.infrastructure.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codeicator.domain.Aggregate;
import com.codeicator.infrastructure.OutboxEvent;
import com.codeicator.infrastructure.OutboxStore;
import com.codeicator.messages.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.Id;
import java.util.List;
import java.util.UUID;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.jpa.repository.JpaRepository;

/** Topic stamping on outbox rows: explicit topic wins, otherwise {@code <Aggregate>.<Event>}. */
class JpaDataPersistentWithEventStoreTest {

  @SuperBuilder(toBuilder = true)
  private static class TestEvent extends Event {}

  private static class TestDomain extends Aggregate.Domain {
    @Id private UUID id;
  }

  @Test
  void stampsExplicitTopicWhenConfigured() {
    assertTopic(new JpaDataPersistentWithEventStore<>(repo(), new ObjectMapper(), store, "tenant.events"), "tenant.events");
  }

  @Test
  void derivesTopicFromAggregateAndEventNamesWhenNotConfigured() {
    assertTopic(
        new JpaDataPersistentWithEventStore<>(repo(), new ObjectMapper(), store),
        "TestDomain.TestEvent");
  }

  private final OutboxStore store = mock(OutboxStore.class);

  @SuppressWarnings("unchecked")
  private JpaRepository<TestDomain, ?> repo() {
    JpaRepository<TestDomain, ?> repo = mock(JpaRepository.class);
    when(repo.save(org.mockito.ArgumentMatchers.any())).thenAnswer(inv -> inv.getArgument(0));
    return repo;
  }

  private void assertTopic(JpaDataPersistentWithEventStore<TestDomain> persistent, String expected) {
    TestDomain domain = new TestDomain();
    domain.id = UUID.randomUUID();
    domain.raiseDomainEvent(
        TestEvent.builder()
            .streamId(domain.id.toString())
            .streamType(TestDomain.class.getName())
            .correlationId("corr-1")
            .orderId(1)
            .build());

    persistent.persist(domain);

    ArgumentCaptor<List<OutboxEvent>> captor = ArgumentCaptor.forClass(List.class);
    verify(store).saveAll(captor.capture());
    assertEquals(expected, captor.getValue().getFirst().getTopic());
  }
}
