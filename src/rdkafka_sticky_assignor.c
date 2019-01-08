/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2015 Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#include "rdkafka_int.h"
#include "rdkafka_assignor.h"

/**
 * Source: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/StickyAssignor.java
 *
 * The sticky assignor serves two purposes. First, it guarantees an assignment
 * that is as balanced as possible, meaning either:

 * the numbers of topic partitions assigned to consumers differ by at most one;
 * or each consumer that has 2+ fewer topic partitions than some other consumer
 * cannot get any of those topic partitions transferred to it. Second, it
 * preserved as many existing assignment as possible when a reassignment occurs.
 * This helps in saving some of the overhead processing when topic partitions
 * move from one consumer to another. Starting fresh it would work by
 * distributing the partitions over consumers as evenly as possible. Even though
 * this may sound similar to how round robin assignor works, the second example
 * below shows that it is not. During a reassignment it would perform the
 * reassignment in such a way that in the new assignment topic partitions are
 * still distributed as evenly as possible, and topic partitions stay with their
 * previously assigned consumers as much as possible. Of course, the first goal
 * above takes precedence over the second one.


 * Example 1. Suppose there are three consumers C0, C1, C2, four topics t0,
 * t1, t2, t3, and each topic has 2 partitions, resulting in partitions t0p0,
 * t0p1, t1p0, t1p1, t2p0, t2p1, t3p0, t3p1. Each consumer is subscribed to all
 * three topics. The assignment with both sticky and round robin assignors will
 * be:

 * C0: [t0p0, t1p1, t3p0]
 * C1: [t0p1, t2p0, t3p1]
 * C2: [t1p0, t2p1]
 *
 * Now, let's assume C1 is removed and a reassignment is about to happen. The
 * round robin assignor would produce: C0: [t0p0, t1p0, t2p0, t3p0] C2: [t0p1,
 * t1p1, t2p1, t3p1] while the sticky assignor would result in: C0 [t0p0, t1p1,
 * t3p0, t2p0] C2 [t1p0, t2p1, t0p1, t3p1] preserving all the previous
 * assignments (unlike the round robin assignor).
 *
 * Example 2. There are three consumers C0, C1, C2, and three topics t0, t1, t2,
 * with 1, 2, and 3 partitions respectively. Therefore, the partitions are t0p0,
 * t1p0, t1p1, t2p0, t2p1, t2p2. C0 is subscribed to t0; C1 is subscribed to t0,
 * t1; and C2 is subscribed to t0, t1, t2. The round robin assignor would come
 * up with the following assignment:
 *
 * C0 [t0p0]
 * C1 [t1p0]
 * C2 [t1p1, t2p0, t2p1, t2p2]
 *
 * which is not as balanced as the assignment suggested by sticky assignor:
 *
 * C0 [t0p0]
 * C1 [t1p0, t1p1]
 * C2 [t2p0, t2p1, t2p2]
 *
 * Now, if consumer C0 is removed, these two assignors would produce the
 * following assignments.
 *
 * Round Robin (preserves 3 partition assignments):
 * C1 [t0p0, t1p1]
 * C2 [t1p0, t2p0, t2p1, t2p2]
 *
 * Sticky (preserves 5 partition assignments):
 * C1 [t1p0, t1p1, t0p0]
 * C2 [t2p0, t2p1, t2p2]
 */

rd_kafka_resp_err_t
rd_kafka_sticky_assignor_assign_cb (rd_kafka_t *rk,
					const char *member_id,
					const char *protocol_name,
					const rd_kafka_metadata_t *metadata,
					rd_kafka_group_member_t *members,
					size_t member_cnt,
					rd_kafka_assignor_topic_t
					**eligible_topics,
					size_t eligible_topic_cnt,
					char *errstr, size_t errstr_size,
					void *opaque) {
        unsigned int ti;
	int next = 0; /* Next member id */

	/* Sort topics by name */
	qsort(eligible_topics, eligible_topic_cnt, sizeof(*eligible_topics),
	      rd_kafka_assignor_topic_cmp);

	/* Sort members by member id */
	qsort(members, member_cnt, sizeof(*members),
	      rd_kafka_group_member_cmp);

        for (ti = 0 ; ti < eligible_topic_cnt ; ti++) {
                rd_kafka_assignor_topic_t *eligible_topic = eligible_topics[ti];
		int partition;

		/* For each topic+partition, assign one member (in a cyclic
		 * iteration) per partition until the partitions are exhausted*/
		for (partition = 0 ;
		     partition < eligible_topic->metadata->partition_cnt ;
		     partition++) {
			rd_kafka_group_member_t *rkgm;

			/* Scan through members until we find one with a
			 * subscription to this topic. */
			while (!rd_kafka_group_member_find_subscription(
				       rk, &members[next],
				       eligible_topic->metadata->topic))
				next++;

			rkgm = &members[next];

			rd_kafka_dbg(rk, CGRP, "ASSIGN",
				     "roundrobin: Member \"%s\": "
				     "assigned topic %s partition %d",
				     rkgm->rkgm_member_id->str,
				     eligible_topic->metadata->topic,
				     partition);

			rd_kafka_topic_partition_list_add(
				rkgm->rkgm_assignment,
				eligible_topic->metadata->topic, partition);

			next = (next+1) % rd_list_cnt(&eligible_topic->members);
		}
	}


        return 0;
}



