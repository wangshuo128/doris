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

#pragma once

#include "obj_store_accesor.h"

#include "recycler/hdfs.h"
#include "recycler/path.h"

namespace doris {
class THdfsParams;

namespace cloud {
class HdfsHandler;

class HdfsAccessor : public ObjStoreAccessor {
public:
    static int create(const THdfsParams& hdfs_params, std::string fs_name, std::string id,
                      std::shared_ptr<HdfsAccessor>* fs, std::string root_path = "");

    // explicit HdfsAccessor(HdfsVaultInfo vault_info);
    HdfsAccessor(const THdfsParams& hdfs_params, std::string fs_name, std::string id,
                 std::string root_path);
    ~HdfsAccessor() = default;

    const std::string& path() const override { return _root_path_str; }

    // const cloud::HdfsVaultInfo& vaultInfo() const { return vault_info_; }

    // returns 0 for success otherwise error
    int init() override;

    // returns 0 for success, returns 1 for http FORBIDDEN error, negative for other errors
    int delete_objects_by_prefix(const std::string& relative_path) override;

    // returns 0 for success otherwise error
    int delete_objects(const std::vector<std::string>& relative_paths) override;

    // returns 0 for success otherwise error
    int delete_object(const std::string& relative_path) override;

    // for test
    // returns 0 for success otherwise error
    int put_object(const std::string& relative_path, const std::string& content) override;

    // returns 0 for success otherwise error
    int list(const std::string& relative_path, std::vector<ObjectMeta>* ObjectMeta) override;

    // return 0 if object exists, 1 if object is not found, otherwise error
    int exist(const std::string& relative_path) override;

    // delete objects which last modified time is less than the input expired time and under the input relative path
    // returns 0 for success otherwise error
    int delete_expired_objects(const std::string& relative_path, int64_t expired_time) override;

    // returns 0 for success otherwise error
    int get_bucket_lifecycle(int64_t* expiration_days) override;

    // returns 0 for enabling bucket versioning, otherwise error
    int check_bucket_versioning() override;

private:
    Path absolute_path(const Path& path) {
        if (path.is_absolute()) {
            return path;
        }
        return _root_path / path;
    }

    int exists(const Path& path, bool* res) const;

    const THdfsParams& _hdfs_params;
    std::string _fs_name;
    std::string _id;
    std::string _root_path_str;
    Path _root_path;
    HdfsHandler* _fs_handle = nullptr;
};

} // namespace cloud
} // namespace doris