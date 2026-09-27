/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.index.sai.postings;

/**
 * A {@link PostingList} whose postings carry token positions. Produced by the analyzed write path
 * so writers can stream one term's positions next to its postings.
 */
public interface PostingListWithPositions extends PostingList
{
    /**
     * Returns the ascending token positions of the posting most recently returned by
     * {@link #nextPosting()}. Only valid after a {@link #nextPosting()} call that did not return
     * {@link #END_OF_STREAM}.
     */
    int[] positionsForCurrentPosting();
}
