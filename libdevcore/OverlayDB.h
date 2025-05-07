/*
    This file is part of cpp-ethereum.

    cpp-ethereum is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    cpp-ethereum is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with cpp-ethereum.  If not, see <http://www.gnu.org/licenses/>.
*/
/** @file OverlayDB.h
 * @author Gav Wood <i@gavwood.com>
 * @date 2014
 */

#pragma once

#include "MemoryDB.h"
#include "dbfwd.h"
#include <memory>

namespace dev
{
class OverlayDB : public MemoryDB
{
public:
    explicit OverlayDB(std::shared_ptr<db::DatabaseFace> _db = nullptr) { m_db = _db; }


    ~OverlayDB();

    // Copyable
    OverlayDB(OverlayDB const&) = default;
    OverlayDB& operator=(OverlayDB const&) = default;
    // Movable
    OverlayDB(OverlayDB&&) = default;
    OverlayDB& operator=(OverlayDB&&) = default;

    void commit();
    // 2024/10/24 与编码块一起提交
    void commit(std::unordered_map<h256, std::string> totalEncodedData, std::vector<std::vector<h256>> partition_result, int node_index);
    // 2025/01/09 仅仅与编码块一起提交
    void commit(std::unordered_map<h256, std::string> totalEncodedData);

    void rollback();

    std::string lookup(h256 const& _h) const;
    bool exists(h256 const& _h) const;
    void kill(h256 const& _h);

    bytes lookupAux(h256 const& _h) const;

private:
    using MemoryDB::clear;

    std::shared_ptr<db::DatabaseFace> m_db;
};

}  // namespace dev
