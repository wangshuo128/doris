// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "recycler/hdfs_builder.h"

#include <fmt/format.h>
#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>

#include <cstdlib>
#include <fstream>
#include <utility>
#include <vector>

#include "common/config.h"
#include "recycler/hdfs.h"
#include "recycler/hdfs_util.h"

namespace doris::cloud {

int HDFSCommonBuilder::init_hdfs_builder() {
    hdfs_builder = hdfsNewBuilder();
    if (hdfs_builder == nullptr) {
        LOG(WARNING)
                << "failed to init HDFSCommonBuilder, please check check be/conf/hdfs-site.xml";
        return -1;
    }
    hdfsBuilderSetForceNewInstance(hdfs_builder);
    return 0;
}

int HDFSCommonBuilder::check_krb_params() {
    std::string ticket_path = config::kerberos_ccache_path;
    if (!ticket_path.empty()) {
        hdfsBuilderConfSetStr(hdfs_builder, "hadoop.security.kerberos.ticket.cache.path",
                              ticket_path.c_str());
        return 0;
    }
    // we should check hdfs_kerberos_principal and hdfs_kerberos_keytab nonnull to login kdc.
    if (hdfs_kerberos_principal.empty() || hdfs_kerberos_keytab.empty()) {
        LOG(WARNING) << "Invalid hdfs_kerberos_principal or hdfs_kerberos_keytab";
        return -1;
    }
    // enable auto-renew thread
    hdfsBuilderConfSetStr(hdfs_builder, "hadoop.kerberos.keytab.login.autorenewal.enabled", "true");
    return 0;
}

int create_hdfs_builder(const THdfsParams& hdfsParams, const std::string& fs_name,
                        HDFSCommonBuilder* builder) {
    FS_RETURN_IF_ERROR(builder->init_hdfs_builder());
    hdfsBuilderSetNameNode(builder->get(), fs_name.c_str());
    // set kerberos conf
    if (hdfsParams.__isset.hdfs_kerberos_keytab) {
        builder->kerberos_login = true;
        builder->hdfs_kerberos_keytab = hdfsParams.hdfs_kerberos_keytab;
#ifdef USE_HADOOP_HDFS
        hdfsBuilderSetKerb5Conf(builder->get(), config::kerberos_krb5_conf_path.c_str());
        hdfsBuilderSetKeyTabFile(builder->get(), hdfsParams.hdfs_kerberos_keytab.c_str());
#endif
    }
    if (hdfsParams.__isset.hdfs_kerberos_principal) {
        builder->kerberos_login = true;
        builder->hdfs_kerberos_principal = hdfsParams.hdfs_kerberos_principal;
        hdfsBuilderSetPrincipal(builder->get(), hdfsParams.hdfs_kerberos_principal.c_str());
    } else if (hdfsParams.__isset.user) {
        hdfsBuilderSetUserName(builder->get(), hdfsParams.user.c_str());
#ifdef USE_HADOOP_HDFS
        hdfsBuilderSetKerb5Conf(builder->get(), nullptr);
        hdfsBuilderSetKeyTabFile(builder->get(), nullptr);
#endif
    }
    // set other conf
    if (hdfsParams.__isset.hdfs_conf) {
        for (const THdfsConf& conf : hdfsParams.hdfs_conf) {
            hdfsBuilderConfSetStr(builder->get(), conf.key.c_str(), conf.value.c_str());
            LOG(INFO) << "set hdfs config: " << conf.key << ", value: " << conf.value;
#ifdef USE_HADOOP_HDFS
            // Set krb5.conf, we should define java.security.krb5.conf in catalog properties
            if (strcmp(conf.key.c_str(), "java.security.krb5.conf") == 0) {
                hdfsBuilderSetKerb5Conf(builder->get(), conf.value.c_str());
            }
#endif
        }
    }
    if (builder->is_kerberos()) {
        FS_RETURN_IF_ERROR(builder->check_krb_params());
    }
    hdfsBuilderConfSetStr(builder->get(), "ipc.client.fallback-to-simple-auth-allowed", "true");
    return 0;
}

} // namespace doris::cloud
