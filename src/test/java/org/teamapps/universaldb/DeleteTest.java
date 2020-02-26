package org.teamapps.universaldb;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.datamodel.testdb1.Company;
import org.teamapps.datamodel.testdb1.FieldTest;
import org.teamapps.datamodel.testdb1.Person;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeleteTest {

    @BeforeClass
    public static void init() throws Exception {
        TestBase.init();
    }

    @Test
    public void testDeleteRecord() {
        Person.getAll().forEach(p -> p.delete());
        Person p1 = Person.create().setLastName("p1").save();
        assertEquals(1, Person.getCount());
        p1.delete();
        assertEquals(0, Person.getCount());
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


}
