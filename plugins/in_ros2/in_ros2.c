/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2022 The Fluent Bit Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include "in_ros2.h"

/* Configuration properties map */
static struct flb_config_map config_map[] = {
    {FLB_CONFIG_MAP_SLIST, "topics", NULL, 0, FLB_TRUE,
     offsetof(struct flb_ros2, topics),
     "Set the topics to subscribe to. Use a space separated string"},
    {FLB_CONFIG_MAP_STR, "node_name", "fluentbit_rclc", 0, FLB_TRUE,
     offsetof(struct flb_ros2, node_name), "Set the name of the node"},
    {FLB_CONFIG_MAP_INT, "spin_time", "100", 0, FLB_TRUE,
     offsetof(struct flb_ros2, spin_time), "Time to spin to wait for data"},
    /* EOF */
    {0}};

// Ideally we pass pointers and not use global variables
rclc_executor_t executor;
rcl_node_t node;
rcl_init_options_t init_options;
rcl_context_t context;
rcl_allocator_t allocator;
rcl_node_options_t node_ops;
void *glob_ctx;
static volatile bool new_data_flag = false;

static int set_timestamp(
    msgpack_packer *mp_pck,
    const windrose_data_collection_interfaces__msg__StringStamped *msg) {
  struct flb_time msg_time = {.tm.tv_sec = msg->header.stamp.sec,
                              .tm.tv_nsec = msg->header.stamp.nanosec};
  int ret = flb_time_append_to_msgpack(&msg_time, mp_pck, 0);

  return ret;
}

static inline int
process_pack(msgpack_packer *mp_pck, struct flb_ros2 *ctx, char *data,
             size_t data_size,
             windrose_data_collection_interfaces__msg__StringStamped *msg) {
  size_t off = 0;
  msgpack_unpacked result;
  msgpack_object entry;

  /* Queue the data with time field */
  msgpack_unpacked_init(&result);

  while (msgpack_unpack_next(&result, data, data_size, &off) ==
         MSGPACK_UNPACK_SUCCESS) {
    entry = result.data;

    if (entry.type == MSGPACK_OBJECT_MAP) {
      msgpack_pack_array(mp_pck, 2);
      set_timestamp(mp_pck, msg);
      msgpack_pack_object(mp_pck, entry);
    } else if (entry.type == MSGPACK_OBJECT_ARRAY) {
      msgpack_pack_object(mp_pck, entry);
    } else {
      /*
       * Upon exception, acknowledge the user about the problem but continue
       * working, do not discard valid JSON entries.
       */
      flb_plg_error(ctx->ins, "invalid record found, "
                              "it's not a JSON map or array");
      msgpack_unpacked_destroy(&result);
      return -1;
    }
  }

  msgpack_unpacked_destroy(&result);
  return 0;
}
void data_callback(const void *msgin) {
  windrose_data_collection_interfaces__msg__StringStamped *msg =
      (windrose_data_collection_interfaces__msg__StringStamped *)msgin;
  if (msg == NULL) {
    flb_warn("Callback: msg NULL\n");
  } else {
    flb_debug("Callback: I heard: %d.%d %s\n", msg->header.stamp.sec,
              msg->header.stamp.nanosec, msg->data.data);
    struct flb_ros2 *ctx = glob_ctx;

    msgpack_sbuffer mp_sbuf;
    msgpack_unpacked result;
    msgpack_packer mp_pck;
    int root_type;

    /* Queue the data with time field */
    msgpack_unpacked_init(&result);

    /* Initialize local msgpack buffer */
    msgpack_sbuffer_init(&mp_sbuf);
    msgpack_packer_init(&mp_pck, &mp_sbuf, msgpack_sbuffer_write);

    size_t pack_size;
    char *pack;
    ctx->buf = msg->data.data;
    ctx->buf_len = msg->data.capacity;

    int ret =
        flb_pack_json(ctx->buf, ctx->buf_len, &pack, &pack_size, &root_type);
    if (ret == FLB_ERR_JSON_PART) {
      flb_plg_warn(ctx->ins, "data incomplete, waiting for more...");
      msgpack_sbuffer_destroy(&mp_sbuf);
    } else if (ret == FLB_ERR_JSON_INVAL) {
      flb_plg_warn(ctx->ins, "invalid JSON message, skipping");
      msgpack_sbuffer_destroy(&mp_sbuf);
    } else if (ret == 0) {
      /* Process valid packaged records */
      process_pack(&mp_pck, ctx, pack, pack_size, msg);

      flb_pack_state_reset(&ctx->pack_state);
      flb_pack_state_init(&ctx->pack_state);
      flb_free(pack);

      flb_input_chunk_append_raw(ctx->ins, NULL, 0, mp_sbuf.data, mp_sbuf.size);
      msgpack_sbuffer_destroy(&mp_sbuf);
    }
  }
}

static int ros2_rclc_init() {
  /* Initialize rclc */
  allocator = rcl_get_default_allocator();
  node_ops = rcl_node_get_default_options();

  /* Define ROS context */
  context = rcl_get_zero_initialized_context();

  /* Create init_options */
  rcl_ret_t rc = rcl_init_options_init(&init_options, allocator);
  if (rc != RCL_RET_OK) {
    flb_error("Error rcl_init_options_init.\n");
    return -1;
  }

  /* Create context */
  rc = rcl_init(0, NULL, &init_options, &context);
  if (rc != RCL_RET_OK) {
    flb_error("Error in rcl_init.\n");
    return -1;
  }
  return 0;
}

static int ros2_node_init(struct flb_ros2 *ctx) {
  /* Create node */
  node = rcl_get_zero_initialized_node();
  const char *node_name = ctx->node_name;
  rcl_ret_t rc = rcl_node_init(&node, node_name, "/", &context, &node_ops);
  if (rc != RCL_RET_OK) {
    flb_error("Error in rclc_node_init\n");
    return -1;
  } else {
    flb_info("Started node %s", node_name);
  }
  return 0;
}

static int ros2_subscribers_init(struct flb_ros2 *ctx) {
  struct mk_list *head;
  struct rclc_subscriber *subscriber;
  struct flb_slist_entry *topic = NULL;

  int len = mk_list_size(ctx->topics);
  if (!ctx->topics || len == 0) {
    flb_error("No 'topics' options has been specified.");
    return 0;
  }
  mk_list_init(&ctx->topic_subs);

  mk_list_foreach(head, ctx->topics) {
    topic = mk_list_entry(head, struct flb_slist_entry, _head);
    subscriber = flb_malloc(sizeof(struct rclc_subscriber));
    if (!subscriber) {
      flb_errno();
      return -1;
    }
    subscriber->data_subscription = rcl_get_zero_initialized_subscription();
    subscriber->data_type_support = ROSIDL_GET_MSG_TYPE_SUPPORT(
        windrose_data_collection_interfaces, msg, StringStamped);
    subscriber->subscription_options = rcl_subscription_get_default_options();

    rcl_ret_t rc = rcl_subscription_init(
        &(subscriber->data_subscription), &node, subscriber->data_type_support,
        topic->str, &(subscriber->subscription_options));
    if (rc != RCL_RET_OK) {
      flb_error("Failed to create subscriber %s.\n", topic->str);
      return -1;

    } else {
      flb_info("Created subscriber %s", topic->str);
    }
    // subscriber->index = i;
    if (false == windrose_data_collection_interfaces__msg__StringStamped__init(
                     &(subscriber->data_msg))) {
      flb_error("Failed to init msg.\n");
      return -1;
    }

    mk_list_add(&subscriber->_head, &(ctx->topic_subs));
  }

  return 0;
}

static int ros2_executor_init(struct flb_ros2 *ctx) {
  /* Executor */
  int len = mk_list_size(ctx->topics);
  rclc_executor_init(&executor, &context, len, &allocator);

  /* Add subscriptions to executor */
  struct mk_list *head;
  struct mk_list *tmp;
  struct rclc_subscriber *an_item;
  rcl_ret_t rc;

  mk_list_foreach_safe(head, tmp, &ctx->topic_subs) {
    an_item = mk_list_entry(head, struct rclc_subscriber, _head);
    rc = rclc_executor_add_subscription(&executor, &an_item->data_subscription,
                                        &(an_item->data_msg), &data_callback,
                                        ON_NEW_DATA);
    if (rc != RCL_RET_OK) {
      flb_error("Error in rclc_executor_add_subscription.\n");
    }
  }
  rc = rclc_executor_prepare(&executor);
  if (rc != RCL_RET_OK) {
    flb_error("Error in rclc_executor_prepare.\n");
  }

  return 0;
}

/* cb_collect callback */
static int in_ros2_collect(struct flb_input_instance *ins,
                           struct flb_config *config, void *in_context) {
  struct flb_ros2 *ctx = in_context;
  rclc_executor_spin_some(&executor, RCL_MS_TO_NS(ctx->spin_time));
  return 0;
}

/* Initialize plugin */
static int in_ros2_init(struct flb_input_instance *in,
                        struct flb_config *config, void *data) {
  int ret = -1;
  struct flb_ros2 *ctx = NULL;

  /* Allocate space for the configuration */
  ctx = flb_calloc(1, sizeof(struct flb_ros2));
  glob_ctx = ctx;

  if (!ctx) {
    return -1;
  }
  ctx->ins = in;
  /* Initialize config */
  ret = flb_input_config_map_set(in, (void *)ctx);
  if (ret == -1) {
    return -1;
  }

  ret = ros2_rclc_init();
  if (ret == -1) {
    return -1;
  }

  ret = ros2_node_init(ctx);
  if (ret == -1) {
    return -1;
  }

  ret = ros2_subscribers_init(ctx);
  if (ret == -1) {
    return -1;
  }

  ret = ros2_executor_init(ctx);
  if (ret == -1) {
    return -1;
  }

  /* Always initialize built-in JSON pack state */
  flb_pack_state_init(&ctx->pack_state);
  /* Load fluentbit context config */
  flb_input_set_context(in, ctx);
  /* Fluentbit collect */
  ret = flb_input_set_collector_time(in, in_ros2_collect, 0, 1000000, config);
  if (ret < 0) {
    flb_plg_error(ctx->ins, "could not set collector for ros2 input plugin");
    flb_free(ctx->topics);
    return -1;
  }
  ctx->coll_fd = 0;

  return 0;
}

static int in_ros2_exit(void *data, struct flb_config *config) {
  /* Clean up */
  rcl_ret_t rc = RCL_RET_OK;
  struct rclc_subscriber *an_item;
  struct mk_list *head;
  struct mk_list *tmp;
  struct flb_ros2 *ctx = data;

  mk_list_foreach_safe(head, tmp, &ctx->topic_subs) {
    an_item = mk_list_entry(head, struct rclc_subscriber, _head);
    rc = rcl_subscription_fini(&an_item->data_subscription, &node);
  }
  rc += rcl_node_fini(&node);
  rc += rcl_init_options_fini(&init_options);
  mk_list_foreach_safe(head, tmp, &ctx->topic_subs) {
    an_item = mk_list_entry(head, struct rclc_subscriber, _head);
    windrose_data_collection_interfaces__msg__StringStamped__fini(
        &(an_item->data_msg));
  }
  rc += rclc_executor_fini(&executor);

  if (rc != RCL_RET_OK) {
    flb_error("Error while cleaning up!\n");
    return -1;
  }
  return 0;
}

struct flb_input_plugin in_ros2_plugin = {.name = "ROS2",
                                          .description = "ROS2 Input",
                                          .cb_init = in_ros2_init,
                                          .cb_pre_run = NULL,
                                          .cb_collect = in_ros2_collect,
                                          .cb_flush_buf = NULL,
                                          .cb_exit = in_ros2_exit,
                                          .config_map = config_map};
