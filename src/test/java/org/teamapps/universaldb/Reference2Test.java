/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.datamodel.testdb1.*;
import org.teamapps.universaldb.pojo.Entity;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;


public class Reference2Test {

	@BeforeClass
	public static void init() throws Exception {
		TestBase.init();
	}

	@Before
	public void cleanTable() {
		Project.getAll().forEach(Entity::delete);
		Milestone.getAll().forEach(Entity::delete);
		Member.getAll().forEach(Entity::delete);
		CompanyCar.getAll().forEach(Entity::delete);
	}

	@Test
	public void testSingleSingleRef() {
		BaseData d = new BaseData();
		assertNull(d.member1.getCar());
		assertNull(d.companyCar1.getOwner());

		d.member1.setCar(d.companyCar1).save();
		assertEquals(d.companyCar1, d.member1.getCar());
		assertEquals(d.member1, d.companyCar1.getOwner());

		assertNull(d.member2.getCar());
		d.member2.setCar(d.companyCar1).save();
		assertEquals(d.companyCar1, d.member2.getCar());
		assertEquals(d.member2, d.companyCar1.getOwner());
		assertNull(d.member1.getCar());

		assertNull(d.member3.getCar());
		assertNull(d.member4.getCar());
		assertNull(d.companyCar3.getOwner());
		assertNull(d.companyCar4.getOwner());
		d.member3.setCar(d.companyCar3).save();
		d.member4.setCar(d.companyCar4).save();
		assertEquals(d.companyCar3, d.member3.getCar());
		assertEquals(d.companyCar4, d.member4.getCar());
		assertEquals(d.member3, d.companyCar3.getOwner());
		assertEquals(d.member4, d.companyCar4.getOwner());

		d.companyCar4.setOwner(d.member3).save();
		assertEquals(d.companyCar4, d.member3.getCar());
		assertEquals(d.member3, d.companyCar4.getOwner());
		assertNull(d.companyCar3.getOwner());
		assertNull(d.member4.getCar());

		d.companyCar3.setOwner(d.member4).save();
		assertEquals(d.companyCar3, d.member4.getCar());
		assertEquals(d.member4, d.companyCar3.getOwner());
		assertEquals(d.companyCar4, d.member3.getCar());
		assertEquals(d.member3, d.companyCar4.getOwner());
	}

	@Test
	public void testSingleMultiRef() {
		BaseData d = new BaseData();
		assertNull(d.milestone1.getProject());
		assertTrue(d.project1.getMilestones().isEmpty());

		d.milestone1.setProject(d.project1).save();
		assertEquals(d.project1, d.milestone1.getProject());
		assertEquals(1, d.project1.getMilestonesCount());
		assertEquals(d.milestone1, d.project1.getMilestones().getFirst());

		d.milestone2.setProject(d.project1).save();
		assertEquals(d.project1, d.milestone2.getProject());
		assertEquals(2, d.project1.getMilestonesCount());
		assertEquals(d.milestone2, d.project1.getMilestones().getLast());

		d.milestone1.setProject(d.project2).save();
		assertEquals(d.project2, d.milestone1.getProject());
		assertEquals(1, d.project1.getMilestonesCount());
		assertEquals(1, d.project2.getMilestonesCount());
		assertEquals(d.milestone1, d.project2.getMilestones().getFirst());
		assertEquals(d.milestone2, d.project1.getMilestones().getFirst());

		d = new BaseData();
		d.project1.addMilestones(d.milestone1, d.milestone2, d.milestone3, d.milestone4).save();
		assertEquals(4, d.project1.getMilestonesCount());
		assertEquals(d.project1, d.milestone1.getProject());
		assertEquals(d.project1, d.milestone2.getProject());
		assertEquals(d.project1, d.milestone3.getProject());
		assertEquals(d.project1, d.milestone4.getProject());

		d.project1.removeMilestones(d.milestone1, d.milestone4).save();
		assertEquals(2, d.project1.getMilestonesCount());
		assertEquals(d.project1, d.milestone2.getProject());
		assertEquals(d.project1, d.milestone3.getProject());
		assertNull(d.milestone1.getProject());
		assertNull(d.milestone4.getProject());

		d = new BaseData();
		d.project1.addMilestones(d.milestone1, d.milestone2, d.milestone3, d.milestone4).save();
		assertEquals(4, d.project1.getMilestonesCount());
		assertTrue(checkEntitySet(d.project1.getMilestones(), d.milestone1, d.milestone2, d.milestone3, d.milestone4));
		assertEquals(d.project1, d.milestone1.getProject());
		assertEquals(d.project1, d.milestone2.getProject());
		assertEquals(d.project1, d.milestone3.getProject());
		assertEquals(d.project1, d.milestone4.getProject());

		d.project2.setMilestones(d.milestone1, d.milestone3).save();
		assertEquals(2, d.project1.getMilestonesCount());
		assertEquals(2, d.project2.getMilestonesCount());
		assertTrue(checkEntitySet(d.project1.getMilestones(), d.milestone2, d.milestone4));
		assertTrue(checkEntitySet(d.project2.getMilestones(), d.milestone1, d.milestone3));
		assertEquals(d.project2, d.milestone1.getProject());
		assertEquals(d.project1, d.milestone2.getProject());
		assertEquals(d.project2, d.milestone3.getProject());
		assertEquals(d.project1, d.milestone4.getProject());

		d.milestone1.setProject(d.project1).save();
		d.milestone3.setProject(d.project1).save();
		assertEquals(4, d.project1.getMilestonesCount());
		assertTrue(checkEntitySet(d.project1.getMilestones(), d.milestone1, d.milestone2, d.milestone3, d.milestone4));
		assertEquals(0, d.project2.getMilestonesCount());
		assertEquals(d.project1, d.milestone1.getProject());
		assertEquals(d.project1, d.milestone2.getProject());
		assertEquals(d.project1, d.milestone3.getProject());
		assertEquals(d.project1, d.milestone4.getProject());
	}

	@Test
	public void testMultiMultiRef() {
		BaseData d = new BaseData();

		d.project1.addMembers(d.member1, d.member2, d.member3).save();
		d.project2.addMembers(d.member2, d.member4).save();
		assertTrue(checkEntitySet(d.project1.getMembers(), d.member1, d.member2, d.member3));
		assertTrue(checkEntitySet(d.project2.getMembers(), d.member2, d.member4));
		assertTrue(checkEntitySet(d.member1.getProjects(), d.project1));
		assertTrue(checkEntitySet(d.member2.getProjects(), d.project1, d.project2));
		assertTrue(checkEntitySet(d.member3.getProjects(), d.project1));
		assertTrue(checkEntitySet(d.member4.getProjects(), d.project2));

		d.project1.removeMembers(d.member1, d.member3).save();
		assertTrue(checkEntitySet(d.project1.getMembers(), d.member2));
		assertTrue(checkEntitySet(d.project2.getMembers(), d.member2, d.member4));
		assertTrue(d.member1.getProjects().isEmpty());
		assertTrue(checkEntitySet(d.member2.getProjects(), d.project1, d.project2));
		assertTrue(d.member3.getProjects().isEmpty());


	}

	private boolean checkEntitySet(List<? extends Entity<?>> set, Entity<?>... entities) {
		if (set.size() != entities.length) return false;
		for (Entity<?> entity : entities) {
			if (!set.contains(entity)) return false;
		}
		return true;
	}

	private static class BaseData {
		public Project project1;
		public Project project2;
		public Project project3;
		public Project project4;
		public Milestone milestone1;
		public Milestone milestone2;
		public Milestone milestone3;
		public Milestone milestone4;
		public Member member1;
		public Member member2;
		public Member member3;
		public Member member4;
		public CompanyCar companyCar1;
		public CompanyCar companyCar2;
		public CompanyCar companyCar3;
		public CompanyCar companyCar4;
		public BaseData() {
			project1 = Project.create().setProjectName("project1").save();
			project2 = Project.create().setProjectName("project2").save();
			project3 = Project.create().setProjectName("project3").save();
			project4 = Project.create().setProjectName("project4").save();
			milestone1 = Milestone.create().setMilestoneName("milestone1").save();
			milestone2 = Milestone.create().setMilestoneName("milestone2").save();
			milestone3 = Milestone.create().setMilestoneName("milestone3").save();
			milestone4 = Milestone.create().setMilestoneName("milestone4").save();
			member1 = Member.create().setMemberName("member1").save();
			member2 = Member.create().setMemberName("member2").save();
			member3 = Member.create().setMemberName("member3").save();
			member4 = Member.create().setMemberName("member4").save();
			companyCar1 = CompanyCar.create().setCompanyCarName("companyCar1").save();
			companyCar2 = CompanyCar.create().setCompanyCarName("companyCar2").save();
			companyCar3 = CompanyCar.create().setCompanyCarName("companyCar3").save();
			companyCar4 = CompanyCar.create().setCompanyCarName("companyCar4").save();
		}
	}



}
