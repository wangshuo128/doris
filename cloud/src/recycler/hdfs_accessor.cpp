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

#include "recycler/hdfs_accessor.h"

#include <fmt/format.h>
#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>

#include "recycler/err_utils.h"
#include "recycler/hdfs_util.h"

namespace doris::cloud {

// TODO: use HdfsVaultInfo protobuf, avoid introduce thrift data struct dependency into cloud project.
int HdfsAccessor::create(const THdfsParams& hdfs_params, std::string fs_name, std::string id,
                         std::shared_ptr<HdfsAccessor>* fs, std::string root_path) {
    (*fs).reset(
            new HdfsAccessor(hdfs_params, std::move(fs_name), std::move(id), std::move(root_path)));
    FS_RETURN_IF_ERROR((*fs)->init());
    return 0;
}

//HdfsAccessor::HdfsAccessor(cloud::HdfsVaultInfo vault_info) {
HdfsAccessor::HdfsAccessor(const THdfsParams& hdfs_params, std::string fs_name, std::string id,
                           std::string root_path)
        : _hdfs_params(hdfs_params),
          _fs_name(std::move(fs_name)),
          _id(std::move(id)),
          _root_path_str(std::move(root_path)) {
    _root_path = _root_path_str;
    if (_fs_name.empty()) {
        _fs_name = hdfs_params.fs_name;
    }
}

// returns 0 for success otherwise error
int HdfsAccessor::init() {
    LOG(INFO) << "hdfs accessor init";
    FS_RETURN_IF_ERROR(
            HdfsHandlerCache::instance()->get_connection(_hdfs_params, _fs_name, &_fs_handle));
    if (!_fs_handle) {
        LOG(WARNING) << "failed to init Hdfs handle with, please check hdfs params.";
        return -1;
    }
    return 0;
}

// returns 0 for success, returns 1 for http FORBIDDEN error, negative for other errors
int HdfsAccessor::delete_objects_by_prefix(const std::string& relative_path) {
    auto path = absolute_path(relative_path);
    bool path_exists = true;
    FS_RETURN_IF_ERROR(exists(path, &path_exists));
    if (!path_exists) {
        return -1;
    }

    int res = hdfsDelete(_fs_handle->hdfs_fs, path.string().c_str(), true);
    if (res == -1) {
        LOG(WARNING) << "failed to delete directory " << path.native() << " : " << hdfs_error();
        return -1;
    }
    return 0;
}

// returns 0 for success otherwise error
int HdfsAccessor::delete_objects(const std::vector<std::string>& relative_paths) {
    LOG(WARNING) << "delete_object is not supported for HDFS.";
    return -1;
}

// returns 0 for success otherwise error
int HdfsAccessor::delete_object(const std::string& relative_path) {
    LOG(WARNING) << "delete_object is not supported for HDFS.";
    return -1;
}

// for test
// returns 0 for success otherwise error
int HdfsAccessor::put_object(const std::string& relative_path, const std::string& content) {
    LOG(WARNING) << "put_object is not supported for HDFS.";
    return -1;
}

// returns 0 for success otherwise error
int HdfsAccessor::list(const std::string& relative_path, std::vector<ObjectMeta>* ObjectMeta) {
    LOG(WARNING) << "list is not supported for HDFS.";
    return -1;
}

// return 0 if object exists, 1 if object is not found, otherwise error
int HdfsAccessor::exist(const std::string& relative_path) {
    LOG(WARNING) << "exist is not supported for HDFS.";
    return -1;
}

// delete objects which last modified time is less than the input expired time and under the input relative path
// returns 0 for success otherwise error
int HdfsAccessor::delete_expired_objects(const std::string& relative_path, int64_t expired_time) {
    LOG(WARNING) << "delete_expired_objects is not supported for HDFS.";
    return -1;
}

// returns 0 for success otherwise error
int HdfsAccessor::get_bucket_lifecycle(int64_t* expiration_days) {
    LOG(WARNING) << "get_bucket_lifecycle is not supported for HDFS.";
    return -1;
}

// returns 0 for enabling bucket versioning, otherwise error
int HdfsAccessor::check_bucket_versioning() {
    LOG(WARNING) << "check_bucket_versioning is not supported for HDFS.";
    return -1;
}

int HdfsAccessor::exists(const Path& path, bool* res) const {
    int is_exists = hdfsExists(_fs_handle->hdfs_fs, path.string().c_str());
#ifdef USE_HADOOP_HDFS
    // when calling hdfsExists() and return non-zero code,
    // if errno is ENOENT, which means the file does not exist.
    // if errno is not ENOENT, which means it encounter other error, should return.
    // NOTE: not for libhdfs3 since it only runs on MaxOS, don't have to support it.
    //
    // See details:
    //  https://github.com/apache/hadoop/blob/5cda162a804fb0cfc2a5ac0058ab407662c5fb00/
    //  hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs/hdfs.c#L1923-L1924
    if (is_exists != 0 && errno != ENOENT) {
        char* root_cause = hdfsGetLastExceptionRootCause();
        LOG(WARNING) << fmt::format("failed to check path existence {}: {}", path.native(),
                                    (root_cause ? root_cause : "unknown"));
        return -1;
    }
#endif
    *res = (is_exists == 0);
    return 0;
}

} // namespace doris::cloud
