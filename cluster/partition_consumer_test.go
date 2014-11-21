package cluster

import (
	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PartitionConsumer", func() {
	var client *sarama.Client
	var subject *PartitionConsumer

	BeforeEach(func() {
		var err error

		client, err = newClient()
		Expect(err).NotTo(HaveOccurred())
		subject, err = NewPartitionConsumer(client, testConsumerConfig(), t_TOPIC, t_GROUP, 3, 0)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() int { return len(subject.stream.Events()) }).Should(BeNumerically(">", 10))
	})

	AfterEach(func() {
		if subject != nil {
			subject.Close()
			subject = nil
		}
		if client != nil {
			client.Close()
			subject = nil
		}
	})

	It("should fetch batches of events", func() {
		batch := subject.Fetch()
		Expect(batch).NotTo(BeNil())
		Expect(batch.Topic).To(Equal(t_TOPIC))
		Expect(batch.Partition).To(Equal(int32(3)))
		Expect(len(batch.Events)).To(BeNumerically(">", 10))
	})

	It("should rollback", func() {
		batch := subject.Fetch()
		Expect(batch).NotTo(BeNil())
		Expect(len(batch.Events)).To(BeNumerically(">", 10))

		was := subject.Offset()
		Eventually(func() int { return len(subject.stream.Events()) }).Should(BeNumerically(">", 10))
		batch = subject.Fetch()
		Expect(batch).NotTo(BeNil())

		subject.Rollback(was)
		Expect(subject.Offset()).To(Equal(was))
	})

	It("should close consumer", func() {
		Expect(subject.Close()).To(BeNil())
		subject = nil
	})

})
