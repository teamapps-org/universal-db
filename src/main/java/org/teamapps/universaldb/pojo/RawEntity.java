/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
 * ---
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.teamapps.universaldb.pojo;

import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.numeric.IntegerIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.index.text.TextIndex;
import org.teamapps.universaldb.transaction.Transaction;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RawEntity extends AbstractUdbEntity<RawEntity> {

    public RawEntity(TableIndex tableIndex) {
        super(tableIndex);
    }

    public List<RawEntity> getAll() {
        List<RawEntity> list = new ArrayList<>();
        BitSet records = getTableIndex().getRecords();
        for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
            list.add(build(id));
        }
        return list;
    }

    public String getText(String fieldName) {
        ColumnIndex columnIndex = getTableIndex().getColumnIndex(fieldName);
        return getTextValue((TextIndex) columnIndex);
    }

    public int getInt(String fieldName) {
        ColumnIndex columnIndex = getTableIndex().getColumnIndex(fieldName);
        return getIntValue((IntegerIndex) columnIndex);
    }

    public void setText(String fieldName, String value) {
        ColumnIndex columnIndex = getTableIndex().getColumnIndex(fieldName);
        setTextValue(value, (TextIndex) columnIndex);
    }

    public void setInteger(String fieldName, int value) {
        ColumnIndex columnIndex = getTableIndex().getColumnIndex(fieldName);
        setIntValue(value, (IntegerIndex) columnIndex);
    }

    public void setReference(String fieldName, RawEntity value) {
        ColumnIndex columnIndex = getTableIndex().getColumnIndex(fieldName);
        setSingleReferenceValue((SingleReferenceIndex) columnIndex, value, null);
    }

    public RawEntity(TableIndex tableIndex, int id, boolean createEntity) {
        super(tableIndex, id, createEntity);
    }


    @Override
    public RawEntity save(Transaction transaction, boolean strictChangeVerification) {
        return null;
    }

    @Override
    public RawEntity saveTransactional(boolean strictChangeVerification) {
        return null;
    }

    @Override
    public RawEntity save() {
        save(getTableIndex());
        return this;
    }

    @Override
    public void delete(Transaction transaction) {

    }

    @Override
    public void delete() {
        delete(getTableIndex());
    }

    @Override
    public RawEntity build() {
        return new RawEntity(getTableIndex(), 0, true);
    }

    @Override
    public RawEntity build(int id) {
        return new RawEntity(getTableIndex(), id, false);
    }
}
