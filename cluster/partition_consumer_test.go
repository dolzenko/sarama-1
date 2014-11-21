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
		subject, err = NewPartitionConsumer(client, testConsumerConfig(), t_TOPIC, t_GROUP, 1, 0)
		Expect(err).NotTo(HaveOccurred())
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

	It("should fetch batches of events (if available)", func() {
		batch := subject.Fetch()
		Expect(batch).NotTo(BeNil())
		Expect(batch.Topic).To(Equal(t_TOPIC))
		Expect(batch.Partition).To(Equal(int32(3)))
		Expect(batch.Events).To(HaveLen(2))
		Expect(subject.Fetch()).To(BeNil())
	})

	PIt("should rollback", func() {
		batch := subject.Fetch()
		Expect(batch).NotTo(BeNil())
	})

	It("should close consumer", func() {
		Expect(subject.Close()).To(BeNil())
		subject = nil
	})

})
