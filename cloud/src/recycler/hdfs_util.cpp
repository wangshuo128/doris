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

#include "hdfs_util.h"

#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>

#include <ostream>

//#include "gutil/hash/hash.h"
#include "recycler/err_utils.h"
#include "recycler/hdfs_builder.h"

namespace doris::cloud {

int create_hdfs_fs(const THdfsParams& hdfs_params, const std::string& fs_name, hdfsFS* fs) {
    LOG(INFO) << "create_hdfs_fs";
    HDFSCommonBuilder builder;
    FS_RETURN_IF_ERROR(create_hdfs_builder(hdfs_params, fs_name, &builder));
    hdfsFS hdfs_fs = hdfsBuilderConnect(builder.get());
    if (hdfs_fs == nullptr) {
        LOG(WARNING) << "failed to connect to hdfs " << fs_name << " : " << hdfs_error();
        return -1;
    }
    *fs = hdfs_fs;
    return 0;
}

// uint64 hdfs_hash_code(const THdfsParams& hdfs_params) {
//     uint64 hash_code = 0;
//     hash_code ^= Fingerprint(hdfs_params.fs_name);
//     if (hdfs_params.__isset.user) {
//         hash_code ^= Fingerprint(hdfs_params.user);
//     }
//     if (hdfs_params.__isset.hdfs_kerberos_principal) {
//         hash_code ^= Fingerprint(hdfs_params.hdfs_kerberos_principal);
//     }
//     if (hdfs_params.__isset.hdfs_kerberos_keytab) {
//         hash_code ^= Fingerprint(hdfs_params.hdfs_kerberos_keytab);
//     }
//     if (hdfs_params.__isset.hdfs_conf) {
//         std::map<std::string, std::string> conf_map;
//         for (const auto& conf : hdfs_params.hdfs_conf) {
//             conf_map[conf.key] = conf.value;
//         }
//         for (auto& conf : conf_map) {
//             hash_code ^= Fingerprint(conf.first);
//             hash_code ^= Fingerprint(conf.second);
//         }
//     }
//     return hash_code;
// }

// todo: delete?
void HdfsHandlerCache::_clean_invalid() {
    //std::vector<uint64> removed_handle;
    //for (auto& item : _cache) {
    //    if (item.second->invalid() && item.second->ref_cnt() == 0) {
    //        removed_handle.emplace_back(item.first);
    //    }
    //}
    //for (auto& handle : removed_handle) {
    //    _cache.erase(handle);
    //}
}

void HdfsHandlerCache::_clean_oldest() {
    // uint64_t oldest_time = ULONG_MAX;
    // uint64 oldest = 0;
    // for (auto& item : _cache) {
    //     if (item.second->ref_cnt() == 0 && item.second->last_access_time() < oldest_time) {
    //         oldest_time = item.second->last_access_time();
    //         oldest = item.first;
    //     }
    // }
    // _cache.erase(oldest);
}

int HdfsHandlerCache::get_connection(const THdfsParams& hdfs_params, const std::string& fs_name,
                                     HdfsHandler** fs_handle) {
    LOG(INFO) << "HdfsHandlerCache::get_connection";
    //fixme review this code.
    hdfsFS hdfs_fs = nullptr;
    FS_RETURN_IF_ERROR(create_hdfs_fs(hdfs_params, fs_name, &hdfs_fs));
    std::unique_ptr<HdfsHandler> handle = std::make_unique<HdfsHandler>(hdfs_fs, true);
    handle->inc_ref();
    *fs_handle = handle.get();

//    uint64 hash_code = hdfs_hash_code(hdfs_params);
//    {
//        std::lock_guard<std::mutex> l(_lock);
//        auto it = _cache.find(hash_code);
//        if (it != _cache.end()) {
//            HdfsHandler* handle = it->second.get();
//            if (!handle->invalid()) {
//                handle->inc_ref();
//                *fs_handle = handle;
//                return 0;
//            }
//            // fs handle is invalid, erase it.
//            _cache.erase(it);
//            LOG(INFO) << "erase the hdfs handle, fs name: " << fs_name;
//        }
//
//        // not find in cache, or fs handle is invalid
//        // create a new one and try to put it into cache
//        hdfsFS hdfs_fs = nullptr;
//        FS_RETURN_IF_ERROR(create_hdfs_fs(hdfs_params, fs_name, &hdfs_fs));
//        if (_cache.size() >= MAX_CACHE_HANDLE) {
//            _clean_invalid();
//            _clean_oldest();
//        }
//        if (_cache.size() < MAX_CACHE_HANDLE) {
//            std::unique_ptr<HdfsHandler> handle = std::make_unique<HdfsHandler>(hdfs_fs, true);
//            handle->inc_ref();
//            *fs_handle = handle.get();
//            _cache[hash_code] = std::move(handle);
//        } else {
//            *fs_handle = new HdfsHandler(hdfs_fs, false);
//        }
//    }
    return 0;
}

Path convert_path(const Path& path, const std::string& namenode) {
    Path real_path(path);
    if (path.string().find(namenode) != std::string::npos) {
        std::string real_path_str = path.string().substr(namenode.size());
        real_path = real_path_str;
    }
    return real_path;
}

bool is_hdfs(const std::string& path_or_fs) {
    return path_or_fs.rfind("hdfs://") == 0;
}

//THdfsParams to_hdfs_params(const cloud::HdfsVaultInfo& vault) {
//    THdfsParams params;
//    auto build_conf = vault.build_conf();
//    params.__set_fs_name(build_conf.fs_name());
//    if (build_conf.has_user()) {
//        params.__set_user(build_conf.user());
//    }
//    if (build_conf.has_hdfs_kerberos_principal()) {
//        params.__set_hdfs_kerberos_keytab(build_conf.hdfs_kerberos_principal());
//    }
//    if (build_conf.has_hdfs_kerberos_keytab()) {
//        params.__set_hdfs_kerberos_principal(build_conf.hdfs_kerberos_keytab());
//    }
//    std::vector<THdfsConf> tconfs;
//    for (const auto& confs : vault.build_conf().hdfs_confs()) {
//        THdfsConf conf;
//        conf.__set_key(confs.key());
//        conf.__set_value(confs.value());
//        tconfs.emplace_back(conf);
//    }
//    params.__set_hdfs_conf(tconfs);
//    return params;
//}

} // namespace doris::cloud
