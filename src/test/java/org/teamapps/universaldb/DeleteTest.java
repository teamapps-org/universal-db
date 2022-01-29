/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2022 TeamApps.org
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
package org.teamapps.universaldb;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.datamodel.testdb1.Company;
import org.teamapps.datamodel.testdb1.Person;

import static org.junit.Assert.*;

public class DeleteTest {

    @BeforeClass
    public static void init() throws Exception {
        TestBase.init();
    }

    @Test
    public void testDeleteRecord() {
        Person.getAll().forEach(p -> p.delete());
        Person p1 = Person.create().setLastName("p1").save();
        assertTrue(p1.isStored());
        assertEquals(1, Person.getCount());
        p1.delete();
        assertEquals(0, Person.getCount());
        assertFalse(p1.isStored());
    }

    @Test
    public void testDeleteReferences() {
        Person p1 = Person.create().setLastName("p1").save();
        Person p2 = Person.create().setLastName("p2").save();
        Person p3 = Person.create().setLastName("p3").save();
        Person p4 = Person.create().setLastName("p4").save();
        Company c1 = Company.create().setName("c1").addEmployees(p1, p2, p3).save();
        Company c2 = Company.create().setName("c2").addEmployees(p4).save();

        assertTrue("employess of c1", TestBase.compareEntities(c1.getEmployees(), p1, p2, p3));
        assertTrue("employess of c2", TestBase.compareEntities(c2.getEmployees(), p4));
        assertEquals(p1.getCompany(), c1);
        assertEquals(p2.getCompany(), c1);
        assertEquals(p3.getCompany(), c1);
        assertEquals(p4.getCompany(), c2);

        c1.removeAllEmployees().save();
        assertTrue("p1 has no c", p1.getCompany() == null);
        assertTrue("p2 has no c", p2.getCompany() == null);
        assertTrue("p3 has no c", p3.getCompany() == null);
        assertTrue("p4 has c", p4.getCompany() != null);
        assertEquals("c1 has no employees", 0,  c1.getEmployeesCount());
        assertTrue("employess of c1", TestBase.compareEntities(c1.getEmployees()));

    }

    @Test
    public void testRemoveReferencesOnEntityDeletion() {
        Person p = Person.create().setLastName("p");
        Company c = Company.create().setName("c").addEmployees(p).save();
        assertEquals(p, c.getEmployees().get(0));
        assertEquals(1, c.getEmployeesCount());
        p.delete();
        assertEquals(0, c.getEmployeesCount());

    }




}
