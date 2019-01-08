/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2018, Magnus Edenhill
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

#include "rdkafka.h"
#include "test.h"

static char *make_topic_name (const char *suffix) {
        static const char *base_name = NULL;
        char *name;

        if (base_name == NULL) {
                base_name = strdup (test_mk_topic_name ("sticky_", 1));
        }

        name = malloc (strlen (base_name) + strlen (suffix) + 1);

        strcpy (name, base_name);
        strcat (name, suffix);

        return name;
}

static void subscribe_consumer (rd_kafka_t *consumer, int topic_cnt,
                                const char *topics[]) {
        rd_kafka_topic_partition_list_t *list;
        rd_kafka_resp_err_t err;
        int i;

        list = rd_kafka_topic_partition_list_new (topic_cnt);

        for (i = 0; i < topic_cnt; i++) {
                rd_kafka_topic_partition_list_add (list, topics[i], -1);
        }

        err = rd_kafka_subscribe (consumer, list);

        if (err)
                TEST_FAIL ("Failed to subscribe: %s\n", rd_kafka_err2str (err));

        rd_kafka_topic_partition_list_destroy (list);
}

static int rebalance_call_count = 0;

static void rebalance_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *partitions,
                          void *opaque) {
        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS)
                rd_kafka_assign (rk, partitions);
        else if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS)
                rd_kafka_assign (rk, NULL);

        if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS)
                rebalance_call_count++;
}

static rd_kafka_t *create_consumer (const char *group,
                                    const char *consumer_name) {
        rd_kafka_conf_t *conf;
        char client_id[64];
        char s_timeout[64];
        char errstr[512];
        rd_kafka_t *consumer;

        test_conf_init (&conf, NULL, 60);

        rd_snprintf (client_id, sizeof (client_id), "%s_%s", test_curr_name (),
                     consumer_name);
        rd_snprintf (s_timeout, sizeof (s_timeout), "%d",
                     test_session_timeout_ms);

        test_conf_set (conf, "partition.assignment.strategy", "sticky");
        test_conf_set (conf, "client.id", client_id);
        test_conf_set (conf, "group.id", group);
        test_conf_set (conf, "session.timeout.ms", s_timeout);
        rd_kafka_conf_set_rebalance_cb (conf, rebalance_cb);

        /* test_conf_set(consumer_conf, "debug", "cgrp"); */

        consumer =
                rd_kafka_new (RD_KAFKA_CONSUMER, conf, errstr, sizeof (errstr));

        return consumer;
}

static void await_rebalance (int consumer_cnt, rd_kafka_t *consumers[]) {
        int i;

        rebalance_call_count = 0;

        while (rebalance_call_count < consumer_cnt) {
                for (i = 0; i < consumer_cnt; i++) {
                        test_consumer_poll_no_msgs ("await-rebalance",
                                                    consumers[i], -1, 1000);
                }
        }
}

static char *printable_assignment (int cnt, const char *assignment[]) {
        char *buffer = malloc (cnt * 16);
        char *tmp = buffer;
        int i;

        for (i = 0; i < cnt; i++) {
                tmp += sprintf (tmp, "%s, ", assignment[i]);
        }

        tmp[-2] = '\0';

        return buffer;
}

static const char **format_partitions (
        const rd_kafka_topic_partition_list_t *partitions) {
        const char **array = malloc (partitions->cnt * (sizeof (char *) + 16));
        char *buffer = (char *)(array + partitions->cnt);
        int i;

        for (i = 0; i < partitions->cnt; i++) {
                const char *topic = partitions->elems[i].topic;

                array[i] = buffer;
                sprintf (buffer, "t%sp%d", topic + strlen (topic) - 1,
                         partitions->elems[i].partition);
                buffer += 16;
        }

        return array;
}

static void assert_assignment (const char *group, rd_kafka_t *consumer, int cnt,
                               const char *assignment[]) {
        rd_kafka_topic_partition_list_t *partitions;
        int i;
        const char **partitions_formatted;
        char consumer_name[3] = "C?";

        consumer_name[1] = strrchr (rd_kafka_name (consumer), '#')[-1];

        rd_kafka_assignment (consumer, &partitions);

        partitions_formatted = format_partitions (partitions);

        if (partitions->cnt != cnt) {
                TEST_FAIL_LATER (
                        "Topic/partition assignment mismatch for [%s:%s]: "
                        "expected { %s }, got { %s }\n",
                        group, consumer_name,
                        printable_assignment (cnt, assignment),
                        printable_assignment (partitions->cnt,
                                              partitions_formatted));
                free (partitions_formatted);
                return;
        }

        for (i = 0; i < cnt; i++) {
                if (strcmp (partitions_formatted[i], assignment[i]) != 0) {
                        TEST_FAIL_LATER (
                                "Topic/partition assignment mismatch for "
                                "[%s:%s]: "
                                "expected { %s }, got { %s }\n",
                                group, consumer_name,
                                printable_assignment (cnt, assignment),
                                printable_assignment (partitions->cnt,
                                                      partitions_formatted));
                        break;
                }
        }

        rd_kafka_topic_partition_list_destroy (partitions);
        free (partitions_formatted);
}

static void test_example_1 (void) {
        static const char group[] = "example1";

        const char *t0 = make_topic_name ("example1_t0");
        const char *t1 = make_topic_name ("example1_t1");
        const char *t2 = make_topic_name ("example1_t2");
        const char *t3 = make_topic_name ("example1_t3");

        const char *topics[] = {t0, t1, t2, t3};

        rd_kafka_t *c0 = create_consumer (group, "C0");
        rd_kafka_t *c1 = create_consumer (group, "C1");
        rd_kafka_t *c2 = create_consumer (group, "C2");

        test_create_topic (t0, 2, 1);
        test_create_topic (t1, 2, 1);
        test_create_topic (t2, 2, 1);
        test_create_topic (t3, 2, 1);

        subscribe_consumer (c0, 4, topics);
        subscribe_consumer (c1, 4, topics);
        subscribe_consumer (c2, 4, topics);

        await_rebalance (3, (rd_kafka_t *[]){c0, c1, c2});

        assert_assignment (group, c0, 3,
                           (const char *[]){"t0p0", "t1p1", "t3p0"});
        assert_assignment (group, c1, 3,
                           (const char *[]){"t0p1", "t2p0", "t3p1"});
        assert_assignment (group, c2, 2, (const char *[]){"t1p0", "t2p1"});

        test_consumer_close (c1);
        rd_kafka_destroy (c1);

        await_rebalance (2, (rd_kafka_t *[]){c0, c2});

        assert_assignment (group, c0, 4,
                           (const char *[]){"t0p0", "t1p1", "t3p0", "t2p0"});
        assert_assignment (group, c2, 4,
                           (const char *[]){"t1p0", "t2p1", "t0p1", "t3p1"});

        test_consumer_close (c0);
        test_consumer_close (c2);

        rd_kafka_destroy (c0);
        rd_kafka_destroy (c2);
}

static void test_example_2 (void) {
        static const char group[] = "example2";

        const char *t0 = make_topic_name ("example2_t0");
        const char *t1 = make_topic_name ("example2_t1");
        const char *t2 = make_topic_name ("example2_t2");

        rd_kafka_t *c0 = create_consumer (group, "C0");
        rd_kafka_t *c1 = create_consumer (group, "C1");
        rd_kafka_t *c2 = create_consumer (group, "C2");

        test_create_topic (t0, 1, 1);
        test_create_topic (t1, 2, 1);
        test_create_topic (t2, 3, 1);

        subscribe_consumer (c0, 1, (const char *[]){t0});
        subscribe_consumer (c1, 2, (const char *[]){t0, t1});
        subscribe_consumer (c2, 3, (const char *[]){t0, t1, t2});

        await_rebalance (3, (rd_kafka_t *[]){c0, c1, c2});

        assert_assignment (group, c0, 1, (const char *[]){"t0p0"});
        assert_assignment (group, c1, 2, (const char *[]){"t1p0", "t1p1"});
        assert_assignment (group, c2, 3,
                           (const char *[]){"t2p0", "t2p1", "t2p2"});

        test_consumer_close (c0);
        rd_kafka_destroy (c0);

        await_rebalance (2, (rd_kafka_t *[]){c1, c2});

        assert_assignment (group, c1, 3,
                           (const char *[]){"t1p0", "t1p1", "t0p0"});
        assert_assignment (group, c2, 3,
                           (const char *[]){"t2p1", "t2p1", "t2p2"});

        test_consumer_close (c1);
        test_consumer_close (c2);

        rd_kafka_destroy (c1);
        rd_kafka_destroy (c2);
}

static void test_example_3 (void) {
        static const char group[] = "example3";

        const char *t0 = make_topic_name ("example3_t0");
        const char *t1 = make_topic_name ("example3_t1");

        const char *topics[] = {t0, t1};

        rd_kafka_t *c0 = create_consumer (group, "C0");
        rd_kafka_t *c1 = create_consumer (group, "C1");
        rd_kafka_t *c2 = create_consumer (group, "C2");

        test_create_topic (t0, 2, 1);
        test_create_topic (t1, 2, 1);

        subscribe_consumer (c0, 2, topics);
        subscribe_consumer (c1, 2, topics);

        await_rebalance (2, (rd_kafka_t *[]){c0, c1});

        assert_assignment (group, c0, 2, (const char *[]){"t0p0", "t1p0"});
        assert_assignment (group, c1, 2, (const char *[]){"t0p1", "t1p1"});

        subscribe_consumer (c2, 2, topics);

        await_rebalance (3, (rd_kafka_t *[]){c0, c1, c2});

        assert_assignment (group, c0, 2, (const char *[]){"t0p0", "t1p0"});
        assert_assignment (group, c1, 1, (const char *[]){"t0p1"});
        assert_assignment (group, c2, 1, (const char *[]){"t1p1"});

        test_consumer_close (c0);
        test_consumer_close (c1);
        test_consumer_close (c2);

        rd_kafka_destroy (c0);
        rd_kafka_destroy (c1);
        rd_kafka_destroy (c2);
}

int main_0094_sticky_assignor (int argc, char **argv) {
        test_example_1 ();
        test_example_2 ();
        test_example_3 ();

        TEST_LATER_CHECK ();

        return 0;
}
