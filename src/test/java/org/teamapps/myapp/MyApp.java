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
package org.teamapps.myapp;

import com.google.common.io.Files;
import org.teamapps.myapp.model.myappdb.Project;
import org.teamapps.myapp.model.myappdb.User;
import org.teamapps.myapp.model.myappdb.UserQuery;
import org.teamapps.universaldb.UniversalDB;
import org.teamapps.myapp.model.MyAppSchema;
import org.teamapps.universaldb.index.text.TextFilter;

import java.io.File;
import java.util.List;

public class MyApp {

    public static void main(String[] args) throws Exception {
        startDb();
        createInitialData();
        query1();
        filterQuery1();
    }
    private static void startDb() throws Exception {
        File storagePath = Files.createTempDir();
//        File storagePath = new File("./my-storage/myappdb");
//        if (!storagePath.exists()) {
//            if (!storagePath.mkdirs()) System.out.println("Error creating Database directory!");
//        }
        UniversalDB.createStandalone(storagePath, new MyAppSchema());
    }
    private static void createInitialData() {
        if (User.getCount() > 0) {
            System.out.println("DB already contains data, don't create new entries");
            return;
        }
        User user1 = User.create().setName("FirstUser").save();
        Project project1 = Project.create().setTitle("ProjectOne");
        user1.addProjects(project1).save();
    }
    private static void query1() {
        User.getAll().forEach(user -> {
            user.getProjects().forEach(project -> {
                System.out.println(user.getName()+ " has project " + project.getTitle());
            });
        });
    }
    private static void filterQuery1() {
        UserQuery firstQuery = User.filter().name(TextFilter.termContainsFilter("First"));
        List<User> firstUserList = firstQuery.execute();
        long firstUserProjectCount = Project.filter().filterManager(firstQuery).execute().stream().count();
        System.out.println("There are " + firstUserProjectCount + " projects where the Manager's name contains 'First'");
    }
}
