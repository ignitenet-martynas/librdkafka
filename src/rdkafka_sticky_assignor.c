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
 * Source:
 https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/consumer/StickyAssignor.java
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

/*
 * A data structure used for mapping topic-partitions to consumers they're
 * currently assigned to or can be assigned to.
 */

typedef struct rd_kafka_topic_partition_consumers_s {
        const char *topic;
        int32_t partition;
        const char *consumer;
        size_t consumer_cnt;
        const char **consumers;
} rd_kafka_topic_partition_consumers_t;

typedef struct rd_kafka_simple_topic_partition_s {
        const char *topic;
        int32_t partition;
} rd_kafka_simple_topic_partition_t;

typedef struct rd_kafka_consumer_topic_partitions_s {
        const char *member_id;
        size_t partition_cnt;
        rd_kafka_simple_topic_partition_t *partitions;
} rd_kafka_consumer_topic_partitions_t;


static void
serialize_topic_partition_list (const rd_kafka_topic_partition_list_t *tplist,
                                void **pdata,
                                size_t *psize) {
        int32_t partition;
        char *data, *p;
        size_t size;
        int i;

        size = 0;

        for (i = 0; i < tplist->cnt; i++)
                size += 4 + strlen(tplist->elems[i].topic) + 1;

        data = malloc(size);

        p = data;

        for (i = 0; i < tplist->cnt; i++) {
                partition = tplist->elems[i].partition;
                p[0] = (partition & 0xff000000) >> 24;
                p[1] = (partition & 0x00ff0000) >> 16;
                p[2] = (partition & 0x0000ff00) >> 8;
                p[3] = partition & 0xff;
                strcpy(p + 4, tplist->elems[i].topic);
                p += 4 + strlen(tplist->elems[i].topic) + 1;
        }

        *pdata = data;
        *psize = size;
}

static void
deserialize_topic_partition_list (const void *data,
                                  size_t size,
                                  rd_kafka_topic_partition_list_t *tplist) {
        const char *p, *end;

        if (tplist->cnt != 0) {
                return;
        }

        p = data;
        end = p + size;

        while (p < end) {
                int partition = (unsigned int)p[0] << 24 |
                                (unsigned int)p[1] << 16 |
                                (unsigned int)p[2] << 8 |
                                (unsigned int)p[3];
                p += 4;
                rd_kafka_topic_partition_list_add(tplist, p, partition);
                p += strlen (p) + 1;
        }
}

static rd_kafka_topic_partition_list_t *member_assignment;


void rd_kafka_sticky_assignor_on_assignment_cb (
        const char *member_id,
        rd_kafka_group_member_t *assignment,
        void *opaque) {
        member_assignment = rd_kafka_topic_partition_list_copy(
                assignment->rkgm_assignment);
}

rd_kafkap_bytes_t *
rd_kafka_sticky_assignor_get_metadata_cb (rd_kafka_assignor_t *rkas,
                                          const rd_list_t *topics) {
        rd_kafkap_bytes_t *metadata;

        void *serialized_assignment;
        size_t serialized_assignment_size;

        if (member_assignment != NULL) {
                /* Serialize the assignments */
                serialize_topic_partition_list(member_assignment,
                                               &serialized_assignment,
                                               &serialized_assignment_size);

                /* Free the no longer needed member_assignment */
                rd_kafka_topic_partition_list_destroy (member_assignment);
                member_assignment = NULL;
        } else {
                serialized_assignment = NULL;
                serialized_assignment_size = 0;
        }

        rkas->rkas_userdata = serialized_assignment;
        rkas->rkas_userdata_size = serialized_assignment_size;

        metadata = rd_kafka_assignor_get_metadata (rkas, topics);

        /* serialized_assignment is now copied into metadata, can free it */
        if (serialized_assignment != NULL) {
                free (serialized_assignment);
        }

        return metadata;
}

static void
remove_non_eligible_partitions(rd_kafka_topic_partition_list_t *assignment,
                               rd_list_t *eligible) {
        int i = 0;

        while (i < assignment->cnt) {
                const char *topic = assignment->elems[i].topic;
                int is_eligible = 0;
                int j;

                for (j = 0; j < eligible->rl_cnt; j++) {
                        const rd_kafka_metadata_topic_t *metadata =
                                eligible->rl_elems[i];

                        if (strcmp (topic, metadata->topic) == 0) {
                                is_eligible = 1;
                                break;
                        }
                }

                if (!is_eligible) {
                        rd_kafka_topic_partition_list_del_by_idx (assignment, i);
                } else {
                        i++;
                }
        }
}

static int is_partition_assigned (const char *topic,
                                  int32_t partition,
                                  size_t member_cnt,
                                  rd_kafka_group_member_t *members) {
        size_t i;

        for (i = 0; i < member_cnt; i++)
                if (rd_kafka_topic_partition_list_find (
                            members[i].rkgm_assignment, topic, partition))
                        return 1;

        return 0;
}

static rd_kafka_topic_partition_list_t *
find_unassigned_partitions (rd_kafka_group_member_t *members,
                            size_t member_cnt,
                            rd_kafka_assignor_topic_t **eligible_topics,
                            size_t eligible_topic_cnt) {
        rd_kafka_topic_partition_list_t *unassigned;
        size_t i;
        int32_t partition;

        unassigned = rd_kafka_topic_partition_list_new (32);

        /* Iterate all eligible topics and for each topic/partition check to
         * see if it's assigned or not */
        for (i = 0; i < eligible_topic_cnt; i++)
                for (partition = 0;
                     partition < eligible_topics[i]->metadata->partition_cnt;
                     partition++)
                        if (!is_partition_assigned (
                                    eligible_topics[i]->metadata->topic,
                                    partition, member_cnt, members))
                                rd_kafka_topic_partition_list_add (
                                        unassigned,
                                        eligible_topics[i]->metadata->topic,
                                        partition);

        return unassigned;
}

static void rd_kafka_topic_partition_consumers_destroy (
        rd_kafka_topic_partition_consumers_t *rktpc, size_t cnt) {
        size_t i;

        for (i = 0; i < cnt; i++)
                if (rktpc[i].consumer_cnt > 0)
                        free (rktpc[i].consumers);

        free (rktpc);
}

static const rd_kafka_topic_partition_consumers_t *
rd_kafka_topic_partition_consumers_find (
        const rd_kafka_topic_partition_consumers_t *consumers,
        size_t cnt,
        const char *topic,
        int32_t partition) {
        size_t i;

        for (i = 0; i < cnt; i++)
                if (consumers[i].partition == partition &&
                    strcmp (consumers[i].topic, topic) == 0)
                        return &consumers[i];

        return NULL;
}

/*
 * Returns a malloc()'ed array of rd_kafka_topic_partition_consumers_t, to be
 * destroyed using rd_kafka_topic_partition_consumers_destroy()
 */

static rd_kafka_topic_partition_consumers_t *
get_partition_assigned_consumers (size_t member_cnt,
                                  rd_kafka_group_member_t *members,
                                  size_t *ret_cnt) {
        rd_kafka_topic_partition_consumers_t *rktpc, *current_tpc;
        size_t cnt;
        size_t i;
        int j;

        cnt = 0;

        for (i = 0; i < member_cnt; i++)
                cnt += (size_t)members[i].rkgm_assignment->cnt;

        *ret_cnt = cnt;

        if (cnt == 0)
                return NULL;

        rktpc = calloc (cnt, sizeof (rd_kafka_topic_partition_consumers_t));

        current_tpc = rktpc;

        for (i = 0; i < member_cnt; i++) {
                for (j = 0; j < members[i].rkgm_assignment->cnt; j++) {
                        const rd_kafka_topic_partition_t *tp =
                                &members[i].rkgm_assignment->elems[j];
                        current_tpc->topic = tp->topic;
                        current_tpc->partition = tp->partition;
                        current_tpc->consumer_cnt = 0; /* Not an array */
                        current_tpc->consumer = members[i].rkgm_member_id->str;

                        current_tpc++;
                }
        }

        return rktpc;
}

/* A "map" of topic-partition to all members it can be assigned to */
static rd_kafka_topic_partition_consumers_t *
get_partition_potential_consumers (size_t member_cnt,
                                   rd_kafka_group_member_t *members,
                                   size_t eligible_topic_cnt,
                                   rd_kafka_assignor_topic_t *eligible_topics[],
                                   size_t *ret_cnt) {
        rd_kafka_topic_partition_consumers_t *rktpc, *current_tpc;
        size_t partition_cnt;
        size_t i;
        int32_t partition;
        int j;

        partition_cnt = 0;

        for (i = 0; i < eligible_topic_cnt; i++)
                partition_cnt += eligible_topics[i]->metadata->partition_cnt;

        *ret_cnt = partition_cnt;

        rktpc = calloc (partition_cnt,
                        sizeof (rd_kafka_topic_partition_consumers_t));

        current_tpc = rktpc;
        for (i = 0; i < eligible_topic_cnt; i++)
                for (partition = 0;
                     partition < eligible_topics[i]->metadata->partition_cnt; partition++) {
                        current_tpc->topic =
                                eligible_topics[i]->metadata->topic;
                        current_tpc->partition = partition;
                        current_tpc->consumers =
                                calloc (eligible_topics[i]->members.rl_cnt, sizeof (const char *));
                        for (j = 0; j < eligible_topics[i]->members.rl_cnt; j++) {
                                const rd_kafka_group_member_t *rkgm =
                                        eligible_topics[i]->members.rl_elems[j];
                                current_tpc->consumers[j] =
                                        rkgm->rkgm_member_id->str;
                        }
                        current_tpc->consumer_cnt =
                                eligible_topics[i]->members.rl_cnt;
                        current_tpc++;
                }

        return rktpc;
}

static rd_kafka_consumer_topic_partitions_t *
rd_kafka_consumer_topic_partitions_find (
        rd_kafka_consumer_topic_partitions_t *rkctp,
        size_t cnt,
        const char *member_id) {
        size_t i;

        for (i = 0; i < cnt; i++)
                if (strcmp (member_id, rkctp[i].member_id) == 0)
                        return &rkctp[i];

        return NULL;
}

static void rd_kafka_consumer_topic_partitions_destroy (
        rd_kafka_consumer_topic_partitions_t *rkctp, size_t cnt) {
        size_t i;

        for (i = 0; i < cnt; i++)
                free (rkctp[i].partitions);

        free (rkctp);
}

/* A "map" of member_id to all assignable topic-partitions */
static rd_kafka_consumer_topic_partitions_t *
get_consumer_assignable_partitions (
        size_t member_cnt,
        const rd_kafka_group_member_t *members,
        size_t eligible_topic_cnt,
        const rd_kafka_assignor_topic_t **eligible_topics) {
        rd_kafka_consumer_topic_partitions_t *rkctp;
        size_t max_partitions;
        size_t cnt, i, j;
        int32_t partition;

        /* Find the maximum number of partitions possible so that we don't
         * bother counting how much memory to allocate for each entry */
        max_partitions = 0;
        for (i = 0; i < eligible_topic_cnt; i++)
                max_partitions += eligible_topics[i]->metadata->partition_cnt;

        rkctp = calloc (member_cnt,
                        sizeof (rd_kafka_consumer_topic_partitions_t));

        for (i = 0; i < member_cnt; i++) {
                rkctp[i].member_id = members[i].rkgm_member_id->str;
                rkctp[i].partitions =
                        calloc (max_partitions,
                                sizeof (rd_kafka_simple_topic_partition_t));
                cnt = 0;

                for (j = 0; j < eligible_topic_cnt; j++)
                        if (rd_list_find (&eligible_topics[j]->members,
                                          &members[i],
                                          rd_kafka_group_member_cmp)) {
                                for (partition = 0;
                                     partition <
                                     eligible_topics[j]
                                             ->metadata->partition_cnt;
                                     partition++) {
                                        rkctp[i].partitions[cnt].topic =
                                                eligible_topics[j]
                                                        ->metadata->topic;
                                        rkctp[i].partitions[cnt].partition =
                                                partition;
                                        cnt++;
                                }
                        }

                rkctp[i].partition_cnt = cnt;
        }

        return rkctp;
}

static int
all_subscriptions_full (const rd_kafka_group_member_t *members,
                        size_t member_cnt,
                        size_t eligible_topic_cnt) {
        size_t i;

        for (i = 0; i < member_cnt; i++) {
                size_t subscr_cnt = (size_t)members[i].rkgm_subscription->cnt;

                if (subscr_cnt != eligible_topic_cnt)
                        return 0;
        }

        return 1;
}

static int
rd_kafka_group_member_cmp_assignments(const void *a, const void *b) {
        const rd_kafka_group_member_t *rkgm_a = a, *rkgm_b = b;
        int result;

        result  = rkgm_a->rkgm_assignment->cnt - rkgm_b->rkgm_assignment->cnt;

        if (result == 0) {
                result = strcmp (rkgm_a->rkgm_member_id->str,
                                 rkgm_b->rkgm_member_id->str);
        }

        return result;
}

static int
rd_kafka_group_member_cmp_assignments_desc (const void *a, const void *b) {
        const rd_kafka_group_member_t *rkgm_a = a, *rkgm_b = b;
        int result;

        result = rkgm_b->rkgm_assignment->cnt - rkgm_a->rkgm_assignment->cnt;

        if (result == 0) {
                result = strcmp (rkgm_a->rkgm_member_id->str,
                                 rkgm_b->rkgm_member_id->str);
        }

        return result;
}

static rd_kafka_topic_partition_list_t *get_partitions_sorted_for_round_robin (
        size_t member_cnt, rd_kafka_group_member_t *members,
        size_t partition_cnt, rd_kafka_topic_partition_consumers_t *rktpc) {
        /* Separator */
        size_t i;
        rd_kafka_group_member_t *consumers;
        rd_kafka_topic_partition_list_t *partitions;

        consumers = calloc (member_cnt, sizeof (rd_kafka_group_member_t));
        memset (consumers, 0, member_cnt * sizeof (rd_kafka_group_member_t));

        for (i = 0; i < member_cnt; i++) {
                consumers[i].rkgm_member_id = members[i].rkgm_member_id;
                consumers[i].rkgm_assignment =
                        rd_kafka_topic_partition_list_copy (
                                members[i].rkgm_assignment);
        }

        partitions = rd_kafka_topic_partition_list_new (partition_cnt);

        while (1 == 1) {
                qsort (consumers, member_cnt, sizeof (rd_kafka_group_member_t),
                        rd_kafka_group_member_cmp_assignments_desc);

                if (consumers[0].rkgm_assignment->cnt == 0)
                        break;

                /* Take the first topic off the first consumer's assignments and
                 * add it to our sorted partitions list */
                rd_kafka_topic_partition_list_add (
                        partitions,
                        consumers[0].rkgm_assignment->elems[0].topic,
                        consumers[0].rkgm_assignment->elems[0].partition);

                rd_kafka_topic_partition_list_del_by_idx (
                        consumers[0].rkgm_assignment, 0);
        }

        /* Any partitions missing from the list are just added to the end */
        for (i = 0; i < partition_cnt; i++)
                if (!rd_kafka_topic_partition_list_find (
                            partitions, rktpc[i].topic, rktpc[i].partition))
                        rd_kafka_topic_partition_list_add (
                                partitions, rktpc[i].topic, rktpc[i].partition);

        for (i = 0; i < member_cnt; i++)
                rd_kafka_topic_partition_list_destroy (
                        consumers[i].rkgm_assignment);

        free (consumers);

        return partitions;
}

static int rd_kafka_topic_partition_consumers_cmp_consumer_cnt (const void *a,
                                                                const void *b) {
        const rd_kafka_topic_partition_consumers_t *rktpc_a = a, *rktpc_b = b;
        int result;

        result = rktpc_a->consumer_cnt - rktpc_b->consumer_cnt;

        if (result == 0) {
                result = strcmp (rktpc_a->topic, rktpc_b->topic);

                if (result == 0)
                        result = rktpc_a->partition - rktpc_b->partition;
        }

        return result;
}

static rd_kafka_topic_partition_list_t *
get_partitions_sorted_by_potential_consumers (
        size_t partition_cnt, rd_kafka_topic_partition_consumers_t *rktpc) {

        rd_kafka_topic_partition_list_t *list;
        size_t i;

        /* Sort the partition map */
        qsort (rktpc, partition_cnt,
               sizeof (rd_kafka_topic_partition_consumers_t),
               rd_kafka_topic_partition_consumers_cmp_consumer_cnt);

        /* Now just copy the topic/partition pairs from the sorted map */
        list = rd_kafka_topic_partition_list_new (partition_cnt);

        for (i = 0; i < partition_cnt; i++)
                rd_kafka_topic_partition_list_add (list, rktpc[i].topic,
                                                   rktpc[i].partition);

        return list;
}

static void assign_partition (
        const char *topic,
        int32_t partition,
        size_t consumer_cnt,
        rd_kafka_group_member_t *sorted_consumers,
        rd_kafka_group_member_t *consumers,
        rd_kafka_consumer_topic_partitions_t *consumer_assignable_partitions,
        size_t assigned_partition_cnt,
        rd_kafka_topic_partition_consumers_t *partition_assigned_consumers) {
        /* TODO: implement me */

}

static int partition_can_participate_in_reassignment (
        const char *topic,
        int32_t partition,
        rd_kafka_topic_partition_consumers_t *partition_potential_consumers,
        size_t partition_cnt) {
        /* If a partition has two or more potential consumers it is subject to
         * reassignment. */
        return rd_kafka_topic_partition_consumers_find (
                       partition_potential_consumers, partition_cnt, topic,
                       partition)
                       ->consumer_cnt >= 2;
}

static int consumer_can_participate_in_reassignment (
        rd_kafka_group_member_t *consumer,
        size_t consumer_cnt,
        rd_kafka_consumer_topic_partitions_t *consumer_assignable_partitions,
        size_t partition_cnt,
        rd_kafka_topic_partition_consumers_t *partition_potential_consumers) {
        /* */
        rd_kafka_topic_partition_list_t *current_partitions =
                consumer->rkgm_assignment;
        size_t current_assignment_size = (size_t)current_partitions->cnt;
        size_t max_assignment_size =
                rd_kafka_consumer_topic_partitions_find (
                        consumer_assignable_partitions, consumer_cnt,
                        consumer->rkgm_member_id->str)
                        ->partition_cnt;
        int i;

        /* If a consumer is not assigned all its potential partitions it is
         * subject to reassignment*/
        if (current_assignment_size < max_assignment_size)
                return 1;

        /* If any of the partitions assigned to a consumer is subject to
         * reassignment the consumer itself is subject to reassignment */
        for (i = 0; i < current_partitions->cnt; i++)
                if (partition_can_participate_in_reassignment (
                            current_partitions->elems[i].topic,
                            current_partitions->elems[i].partition,
                            partition_potential_consumers, partition_cnt))
                        return 1;

        return 0;
}

static void partition_movements_move_partition () {
        /* TODO: implement me! */
}

static void process_partition_movement () {
        /* TODO: implement me! */
}

static void reassign_partition(const char *topic, int32_t partition,
        size_t mutable_consumer_cnt, rd_kafka_group_member_t *mutable_consumers,
        rd_kafka_group_member_t *sorted_mutable_consumers,
        size_t partition_cnt,
        rd_kafka_topic_partition_consumers_t *partition_assigned_consumers,
        size_t consumer_cnt,
        rd_kafka_consumer_topic_partitions_t *consumer_assignable_partitions) {
        /* TODO: implement me! */
}

static int is_balanced (size_t mutable_consumer_cnt,
                        rd_kafka_group_member_t *mutable_consumers,
                        rd_kafka_group_member_t *sorted_mutable_consumers,
                        size_t consumer_cnt,
                        rd_kafka_group_member_t *consumers) {
        /* TODO: implement me! */
        return 0;
}

static int perform_reassignments (
        rd_kafka_topic_partition_list_t *reassignable_partitions,
        size_t mutable_consumer_cnt,
        rd_kafka_group_member_t *mutable_consumers,
        rd_kafka_group_member_t *sorted_mutable_consumers,
        size_t consumer_cnt,
        rd_kafka_consumer_topic_partitions_t *consumer_assignable_partitions,
        size_t partition_cnt,
        rd_kafka_topic_partition_consumers_t *partition_potential_consumers,
        size_t assigned_partition_cnt,
        rd_kafka_topic_partition_consumers_t *partition_assigned_consumers) {
        /* TODO: implement me! */
        return 0;
}

static int
get_balance_score (size_t consumer_cnt, rd_kafka_group_member_t *consumers) {
        /* TODO: implement me! */
        return 0;
}

static rd_kafka_group_member_t *
rd_kafka_group_member_find(rd_kafka_group_member_t *members, size_t cnt, const char *member_id) {
        size_t i;

        for (i = 0; i < cnt; i++)
                if (strcmp(members[i].rkgm_member_id->str, member_id) == 0)
                        return &members[i];

        return NULL;
}

static void
balance (size_t consumer_cnt,
         rd_kafka_group_member_t *consumers,
         rd_kafka_topic_partition_list_t *sorted_partitions,
         rd_kafka_topic_partition_list_t *unassigned_partitions,
         rd_kafka_group_member_t *sorted_consumers,
         rd_kafka_consumer_topic_partitions_t *consumer_assignable_partitions,
         size_t partition_cnt,
         rd_kafka_topic_partition_consumers_t *partition_potential_consumers,
         size_t assigned_partition_cnt,
         rd_kafka_topic_partition_consumers_t *partition_assigned_consumers) {
        /* Just a separator */
        int initializing = 0;
        int reassignment_performed = 0;

        rd_kafka_topic_partition_list_t *reassignable_partitions;

        rd_kafka_group_member_t *mutable_consumers;
        size_t mutable_consumer_cnt;

        rd_kafka_group_member_t *mutable_consumers_backup;
        rd_kafka_group_member_t *sorted_mutable_consumers;

        size_t ci;
        int i;

        if (sorted_consumers[consumer_cnt - 1].rkgm_assignment->cnt == 0)
                initializing = 1;


        /* Iterate unassigned partitions and assign them if there's consumers
         * they can be assigned to */
        for (i = 0; i < unassigned_partitions->cnt; i++) {
                /* Skip the partition if there is no potential consumer */
                if (rd_kafka_topic_partition_consumers_find (
                            partition_potential_consumers, partition_cnt,
                            unassigned_partitions->elems[i].topic,
                            unassigned_partitions->elems[i].partition)
                            ->consumer_cnt == 0)
                        continue;

                assign_partition (unassigned_partitions->elems[i].topic,
                                  unassigned_partitions->elems[i].partition,
                                  consumer_cnt, sorted_consumers, consumers,
                                  consumer_assignable_partitions,
                                  assigned_partition_cnt,
                                  partition_assigned_consumers);
        }

        /* Make a list of just the partitions that can be reassigned */
        reassignable_partitions =
                rd_kafka_topic_partition_list_new (partition_cnt);

        for (i = 0; i < sorted_partitions->cnt; i++) {
                const char *topic = sorted_partitions->elems[i].topic;
                int32_t partition = sorted_partitions->elems[i].partition;

                if (partition_can_participate_in_reassignment (
                            topic, partition, partition_potential_consumers,
                            partition_cnt))
                        rd_kafka_topic_partition_list_add (
                                reassignable_partitions, topic, partition);
        }

        mutable_consumer_cnt = 0;
        mutable_consumers =
                calloc (consumer_cnt, sizeof (rd_kafka_group_member_t));

        for (ci = 0; ci < consumer_cnt; ci++)
                if (consumer_can_participate_in_reassignment (
                            &consumers[ci], consumer_cnt,
                            consumer_assignable_partitions, partition_cnt,
                            partition_potential_consumers))
                        mutable_consumers[mutable_consumer_cnt++] =
                                consumers[ci];

        mutable_consumers_backup =
                calloc (mutable_consumer_cnt, sizeof (rd_kafka_group_member_t));
        memcpy (mutable_consumers_backup, mutable_consumers,
                mutable_consumer_cnt * sizeof (rd_kafka_group_member_t));


        sorted_mutable_consumers =
                calloc (mutable_consumer_cnt, sizeof (rd_kafka_group_member_t));

        memcpy (sorted_mutable_consumers, mutable_consumers,
                mutable_consumer_cnt * sizeof (rd_kafka_group_member_t));

        qsort (sorted_mutable_consumers, mutable_consumer_cnt,
               sizeof (rd_kafka_group_member_t),
               rd_kafka_group_member_cmp_assignments);

        for (ci = 0; ci < mutable_consumer_cnt; ci++) {
                mutable_consumers[ci].rkgm_assignment =
                        rd_kafka_topic_partition_list_copy (
                                mutable_consumers[ci].rkgm_assignment);
                mutable_consumers_backup[ci].rkgm_assignment =
                        rd_kafka_topic_partition_list_copy (
                                mutable_consumers_backup[ci].rkgm_assignment);
                sorted_mutable_consumers[ci].rkgm_assignment =
                        rd_kafka_topic_partition_list_copy (
                                sorted_mutable_consumers[ci].rkgm_assignment);
        }

        reassignment_performed = perform_reassignments (
                reassignable_partitions, mutable_consumer_cnt,
                mutable_consumers, sorted_mutable_consumers, consumer_cnt,
                consumer_assignable_partitions, partition_cnt,
                partition_potential_consumers, assigned_partition_cnt,
                partition_assigned_consumers);

        if (initializing || !reassignment_performed ||
            get_balance_score (mutable_consumer_cnt, mutable_consumers) >
                    get_balance_score (mutable_consumer_cnt,
                                       mutable_consumers_backup)) {
                for (ci = 0; ci < mutable_consumer_cnt; ci++) {
                        rd_kafka_group_member_t *consumer;

                        consumer = rd_kafka_group_member_find (
                                consumers, consumer_cnt,
                                mutable_consumers[ci].rkgm_member_id->str);

                        rd_kafka_topic_partition_list_destroy (
                                consumer->rkgm_assignment);
                        consumer->rkgm_assignment =
                                rd_kafka_topic_partition_list_copy (
                                        mutable_consumers[ci].rkgm_assignment);
                }
        }

        /* Free all the resources allocated in this function */

        rd_kafka_topic_partition_list_destroy (reassignable_partitions);

        for (ci = 0; ci < mutable_consumer_cnt; ci++) {
                rd_kafka_topic_partition_list_destroy (
                        mutable_consumers_backup[ci].rkgm_assignment);
                rd_kafka_topic_partition_list_destroy (
                        sorted_mutable_consumers[ci].rkgm_assignment);
                rd_kafka_topic_partition_list_destroy (
                        mutable_consumers[ci].rkgm_assignment);
        }

        free (sorted_mutable_consumers);
        free (mutable_consumers_backup);
        free (mutable_consumers);
}

rd_kafka_resp_err_t rd_kafka_sticky_assignor_assign_cb (
        rd_kafka_t *rk,
        const char *member_id,
        const char *protocol_name,
        const rd_kafka_metadata_t *metadata,
        rd_kafka_group_member_t *members,
        size_t member_cnt,
        rd_kafka_assignor_topic_t **eligible_topics,
        size_t eligible_topic_cnt,
        char *errstr,
        size_t errstr_size,
        void *opaque) {
        rd_kafka_consumer_topic_partitions_t *consumer_assignable_partitions;
        rd_kafka_topic_partition_consumers_t *partition_potential_consumers;

        size_t assigned_partition_cnt, partition_cnt;

        rd_kafka_topic_partition_consumers_t *partition_assigned_consumers;

        rd_kafka_topic_partition_list_t *unassigned_partitions;
        rd_kafka_topic_partition_list_t *sorted_partitions;

        rd_kafka_group_member_t *consumers_asc_by_assignments;

        int is_fresh_assignment = 1;

        size_t i;

        for (i = 0; i < member_cnt; i++) {
                rd_kafka_group_member_t *rkgm = &members[i];

                if (rkgm->rkgm_userdata->len > 0) {
                        deserialize_topic_partition_list (
                                rkgm->rkgm_userdata->data,
                                rkgm->rkgm_userdata->len,
                                rkgm->rkgm_assignment);

                        is_fresh_assignment = 0;

                        /* Remove all partitions that the consumer is no longer
                         * subscribed to */
                        remove_non_eligible_partitions (rkgm->rkgm_assignment,
                                                        &rkgm->rkgm_eligible);

                        /* TODO: */
                        /* Remove all double-assignments that are a result of
                        * as consumer coming back with its old assignments
                        * after they have been already reassigned */
                }
        }

        unassigned_partitions = find_unassigned_partitions (
                members, member_cnt, eligible_topics, eligible_topic_cnt);

        consumer_assignable_partitions = get_consumer_assignable_partitions (
                member_cnt, members, eligible_topic_cnt, eligible_topics);

        partition_potential_consumers = get_partition_potential_consumers (
                member_cnt, members, eligible_topic_cnt, eligible_topics,
                &partition_cnt);

        partition_assigned_consumers = get_partition_assigned_consumers (
                member_cnt, members, &assigned_partition_cnt);

        consumers_asc_by_assignments =
                calloc (member_cnt, sizeof (rd_kafka_group_member_t));

        memcpy (consumers_asc_by_assignments, members,
                member_cnt * sizeof (rd_kafka_group_member_t));

        qsort (consumers_asc_by_assignments, member_cnt,
               sizeof (rd_kafka_group_member_t),
               rd_kafka_group_member_cmp_assignments);

        if (!is_fresh_assignment &&
            all_subscriptions_full (members, member_cnt, eligible_topic_cnt)) {
                sorted_partitions = get_partitions_sorted_for_round_robin (
                        member_cnt, members, partition_cnt,
                        partition_potential_consumers);
        } else {
                sorted_partitions =
                        get_partitions_sorted_by_potential_consumers (
                                partition_cnt, partition_potential_consumers);
        }

        balance (member_cnt, members, sorted_partitions, unassigned_partitions,
                 consumers_asc_by_assignments, consumer_assignable_partitions,
                 partition_cnt, partition_potential_consumers,
                 assigned_partition_cnt,
                 partition_assigned_consumers);

        rd_kafka_topic_partition_consumers_destroy (partition_potential_consumers, partition_cnt);
        rd_kafka_topic_partition_consumers_destroy (partition_assigned_consumers, assigned_partition_cnt);

        rd_kafka_consumer_topic_partitions_destroy (consumer_assignable_partitions, member_cnt);

        rd_kafka_topic_partition_list_destroy (unassigned_partitions);
        rd_kafka_topic_partition_list_destroy (sorted_partitions);

        free (consumers_asc_by_assignments);

        return 0;
}
