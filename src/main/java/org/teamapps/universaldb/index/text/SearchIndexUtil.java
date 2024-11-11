/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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
package org.teamapps.universaldb.index.text;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SearchIndexUtil {

	public static List<String> analyze(String text, Analyzer analyzer) {
		try {
			List<String> result = new ArrayList<>();
			TokenStream tokenStream = analyzer.tokenStream("any", text);
			CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
			tokenStream.reset();
			while (tokenStream.incrementToken()) {
				result.add(attr.toString());
			}
			tokenStream.close();
			return result;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}


	public static Query createQuery(TextFilterType filterType, String fieldName, String value, Analyzer analyzer) {
		List<String> textParts = analyze(value, analyzer);
		BooleanQuery.Builder termQueries = new BooleanQuery.Builder();
		boolean containsOnlyNegationQueries = false;
		for (String textPart : textParts) {
			Term term = new Term(fieldName, textPart);
			switch (filterType) {
				case EMPTY:
				case NOT_EMPTY:
				case TEXT_BYTE_LENGTH_GREATER:
				case TEXT_BYTE_LENGTH_SMALLER:
					return null;
				case TEXT_EQUALS:
				case TEXT_EQUALS_IGNORE_CASE:
				case TERM_EQUALS:
					termQueries.add(new TermQuery(term), BooleanClause.Occur.MUST);
					break;
				case TEXT_NOT_EQUALS:
				case TERM_NOT_EQUALS:
					containsOnlyNegationQueries = true;
					termQueries.add(new TermQuery(term), BooleanClause.Occur.MUST_NOT);
					break;
				case TERM_STARTS_WITH:
					termQueries.add(new PrefixQuery(term), BooleanClause.Occur.MUST);
					break;
				case TERM_STARTS_NOT_WITH:
					containsOnlyNegationQueries = true;
					termQueries.add(new PrefixQuery(term), BooleanClause.Occur.MUST_NOT);
					break;
				case TERM_SIMILAR:
					termQueries.add(new FuzzyQuery(term), BooleanClause.Occur.MUST);
					break;
				case TERM_NOT_SIMILAR:
					containsOnlyNegationQueries = true;
					termQueries.add(new FuzzyQuery(term), BooleanClause.Occur.MUST_NOT);
					break;
				case TERM_CONTAINS:
					termQueries.add(new WildcardQuery(new Term(fieldName, "*" + textPart + "*")), BooleanClause.Occur.MUST);
					break;
				case TERM_CONTAINS_NOT:
					containsOnlyNegationQueries = true;
					termQueries.add(new WildcardQuery(new Term(fieldName, "*" + textPart + "*")), BooleanClause.Occur.MUST_NOT);
					break;
			}
		}
		if (containsOnlyNegationQueries) {
			termQueries.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
		}
		return termQueries.build();
	}

	public static FieldType createIndexFieldType() {
		FieldType fieldType = new FieldType();
		fieldType.setIndexOptions(IndexOptions.DOCS);
		fieldType.setOmitNorms(true);
		fieldType.setStored(false);
		fieldType.setTokenized(true);
		fieldType.freeze();
		return fieldType;
	}
}
