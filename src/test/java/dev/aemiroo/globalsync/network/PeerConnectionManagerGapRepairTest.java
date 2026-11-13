    @Test
    void selectivelyRepairsAMissingOriginSequenceWithoutAdvancingTheNormalCursor() throws Exception {
        int portA = availablePort();
        int portB = availablePort();
        Path nodeA = testDirectory.resolve("gap-origin");
        Path nodeB = testDirectory.resolve("gap-receiver");
        try (SqliteEventJournal journalA = new SqliteEventJournal(nodeA.resolve("events.db"));
             SqliteEventJournal journalB = new SqliteEventJournal(nodeB.resolve("events.db"));
             SqliteReceivedEventStore inboxA = new SqliteReceivedEventStore(nodeA.resolve("received.db"));
             SqliteReceivedEventStore inboxB = new SqliteReceivedEventStore(nodeB.resolve("received.db"))) {
            var event1 = blockEvent("a-region", 1);
            var event2 = blockEvent("a-region", 2);
            var event3 = blockEvent("a-region", 3);
            journalA.append(event1);
            journalA.append(event2);
            journalA.append(event3);
            long remoteOffset = 0;
            for (var entry : journalA.readAfter(0, 10)) {
                journalA.acknowledge("b-region", entry, ++remoteOffset);
            }

            ReplicationMessageHandler receiverInbox = new ReplicationMessageHandler(inboxB, java.util.Set.of("world"));
            receiverInbox.handle(ReplicationMessageCodec.encodeEvent(event1), "a-region");
            receiverInbox.handle(ReplicationMessageCodec.encodeEvent(event3), "a-region");
            assertEquals(2L, receiverInbox.firstMissingSequenceForOrigin("a-region"));

            try (PeerConnectionManager managerA = managerWithDelivery(contextA, "a-region",
                         new PeerEndpoint("b-region", "127.0.0.1", portB, fingerprintB), journalA, inboxA);
                 PeerConnectionManager managerB = managerWithDelivery(contextB, "b-region",
                         new PeerEndpoint("a-region", "127.0.0.1", portA, fingerprintA), journalB, inboxB)) {
                managerB.start(LOOPBACK, portB);
                managerA.start(LOOPBACK, portA);
                managerA.connectConfiguredPeers();

                assertTrue(await(() -> {
                    try {
                        return inboxB.size() == 3
                                && receiverInbox.firstMissingSequenceForOrigin("a-region") == 0;
                    } catch (IOException exception) {
                        return false;
                    }
                }, 10, TimeUnit.SECONDS), "the receiver should request and durably accept the missing sequence");
                assertEquals(3L, journalA.acknowledgedOffset("b-region"),
                        "selective repair must not move or rewrite the ordinary delivery cursor");
            }
        }
    }

