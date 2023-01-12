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

// TPC, TPC Benchmark, and TPC-DS and TPC-VMS are trademarks
// of the Transaction Processing Performance Council.

// Schema and queries used in these tests are copyright
// 2015 Transaction Processing Performance Council

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::fs;
use std::sync::Arc;

#[tokio::test]
async fn tpcds_logical_q1() -> Result<()> {
    create_logical_plan(1).await
}

#[tokio::test]
async fn tpcds_logical_q2() -> Result<()> {
    create_logical_plan(2).await
}

#[tokio::test]
async fn tpcds_logical_q3() -> Result<()> {
    create_logical_plan(3).await
}

#[tokio::test]
async fn tpcds_logical_q4() -> Result<()> {
    create_logical_plan(4).await
}

#[tokio::test]
async fn tpcds_logical_q5() -> Result<()> {
    create_logical_plan(5).await
}

#[tokio::test]
async fn tpcds_logical_q6() -> Result<()> {
    create_logical_plan(6).await
}

#[tokio::test]
async fn tpcds_logical_q7() -> Result<()> {
    create_logical_plan(7).await
}

#[tokio::test]
async fn tpcds_logical_q8() -> Result<()> {
    create_logical_plan(8).await
}

#[tokio::test]
async fn tpcds_logical_q9() -> Result<()> {
    create_logical_plan(9).await
}

#[tokio::test]
async fn tpcds_logical_q10() -> Result<()> {
    create_logical_plan(10).await
}

#[tokio::test]
async fn tpcds_logical_q11() -> Result<()> {
    create_logical_plan(11).await
}

#[tokio::test]
async fn tpcds_logical_q12() -> Result<()> {
    create_logical_plan(12).await
}

#[tokio::test]
async fn tpcds_logical_q13() -> Result<()> {
    create_logical_plan(13).await
}

#[tokio::test]
async fn tpcds_logical_q14() -> Result<()> {
    create_logical_plan(14).await
}

#[tokio::test]
async fn tpcds_logical_q15() -> Result<()> {
    create_logical_plan(15).await
}

#[tokio::test]
async fn tpcds_logical_q16() -> Result<()> {
    create_logical_plan(16).await
}

#[tokio::test]
async fn tpcds_logical_q17() -> Result<()> {
    create_logical_plan(17).await
}

#[tokio::test]
async fn tpcds_logical_q18() -> Result<()> {
    create_logical_plan(18).await
}

#[tokio::test]
async fn tpcds_logical_q19() -> Result<()> {
    create_logical_plan(19).await
}

#[tokio::test]
async fn tpcds_logical_q20() -> Result<()> {
    create_logical_plan(20).await
}

#[tokio::test]
async fn tpcds_logical_q21() -> Result<()> {
    create_logical_plan(21).await
}

#[tokio::test]
async fn tpcds_logical_q22() -> Result<()> {
    create_logical_plan(22).await
}

#[tokio::test]
async fn tpcds_logical_q23() -> Result<()> {
    create_logical_plan(23).await
}

#[tokio::test]
async fn tpcds_logical_q24() -> Result<()> {
    create_logical_plan(24).await
}

#[tokio::test]
async fn tpcds_logical_q25() -> Result<()> {
    create_logical_plan(25).await
}

#[tokio::test]
async fn tpcds_logical_q26() -> Result<()> {
    create_logical_plan(26).await
}

#[tokio::test]
async fn tpcds_logical_q27() -> Result<()> {
    create_logical_plan(27).await
}

#[tokio::test]
async fn tpcds_logical_q28() -> Result<()> {
    create_logical_plan(28).await
}

#[tokio::test]
async fn tpcds_logical_q29() -> Result<()> {
    create_logical_plan(29).await
}

#[tokio::test]
async fn tpcds_logical_q30() -> Result<()> {
    create_logical_plan(30).await
}

#[tokio::test]
async fn tpcds_logical_q31() -> Result<()> {
    create_logical_plan(31).await
}

#[tokio::test]
async fn tpcds_logical_q32() -> Result<()> {
    create_logical_plan(32).await
}

#[tokio::test]
async fn tpcds_logical_q33() -> Result<()> {
    create_logical_plan(33).await
}

#[tokio::test]
async fn tpcds_logical_q34() -> Result<()> {
    create_logical_plan(34).await
}

#[tokio::test]
async fn tpcds_logical_q35() -> Result<()> {
    create_logical_plan(35).await
}

#[tokio::test]
async fn tpcds_logical_q36() -> Result<()> {
    create_logical_plan(36).await
}

#[tokio::test]
async fn tpcds_logical_q37() -> Result<()> {
    create_logical_plan(37).await
}

#[tokio::test]
async fn tpcds_logical_q38() -> Result<()> {
    create_logical_plan(38).await
}

#[tokio::test]
async fn tpcds_logical_q39() -> Result<()> {
    create_logical_plan(39).await
}

#[tokio::test]
async fn tpcds_logical_q40() -> Result<()> {
    create_logical_plan(40).await
}

#[tokio::test]
async fn tpcds_logical_q41() -> Result<()> {
    create_logical_plan(41).await
}

#[tokio::test]
async fn tpcds_logical_q42() -> Result<()> {
    create_logical_plan(42).await
}

#[tokio::test]
async fn tpcds_logical_q43() -> Result<()> {
    create_logical_plan(43).await
}

#[tokio::test]
async fn tpcds_logical_q44() -> Result<()> {
    create_logical_plan(44).await
}

#[tokio::test]
async fn tpcds_logical_q45() -> Result<()> {
    create_logical_plan(45).await
}

#[tokio::test]
async fn tpcds_logical_q46() -> Result<()> {
    create_logical_plan(46).await
}

#[tokio::test]
async fn tpcds_logical_q47() -> Result<()> {
    create_logical_plan(47).await
}

#[tokio::test]
async fn tpcds_logical_q48() -> Result<()> {
    create_logical_plan(48).await
}

#[tokio::test]
async fn tpcds_logical_q49() -> Result<()> {
    create_logical_plan(49).await
}

#[tokio::test]
async fn tpcds_logical_q50() -> Result<()> {
    create_logical_plan(50).await
}

#[tokio::test]
async fn tpcds_logical_q51() -> Result<()> {
    create_logical_plan(51).await
}

#[tokio::test]
async fn tpcds_logical_q52() -> Result<()> {
    create_logical_plan(52).await
}

#[tokio::test]
async fn tpcds_logical_q53() -> Result<()> {
    create_logical_plan(53).await
}

#[tokio::test]
async fn tpcds_logical_q54() -> Result<()> {
    create_logical_plan(54).await
}

#[tokio::test]
async fn tpcds_logical_q55() -> Result<()> {
    create_logical_plan(55).await
}

#[tokio::test]
async fn tpcds_logical_q56() -> Result<()> {
    create_logical_plan(56).await
}

#[tokio::test]
async fn tpcds_logical_q57() -> Result<()> {
    create_logical_plan(57).await
}

#[tokio::test]
async fn tpcds_logical_q58() -> Result<()> {
    create_logical_plan(58).await
}

#[tokio::test]
async fn tpcds_logical_q59() -> Result<()> {
    create_logical_plan(59).await
}

#[tokio::test]
async fn tpcds_logical_q60() -> Result<()> {
    create_logical_plan(60).await
}

#[tokio::test]
async fn tpcds_logical_q61() -> Result<()> {
    create_logical_plan(61).await
}

#[tokio::test]
async fn tpcds_logical_q62() -> Result<()> {
    create_logical_plan(62).await
}

#[tokio::test]
async fn tpcds_logical_q63() -> Result<()> {
    create_logical_plan(63).await
}

#[ignore] // thread 'q64' has overflowed its stack]
#[tokio::test]
async fn tpcds_logical_q64() -> Result<()> {
    create_logical_plan(64).await
}

#[tokio::test]
async fn tpcds_logical_q65() -> Result<()> {
    create_logical_plan(65).await
}

#[tokio::test]
async fn tpcds_logical_q66() -> Result<()> {
    create_logical_plan(66).await
}

#[tokio::test]
async fn tpcds_logical_q67() -> Result<()> {
    create_logical_plan(67).await
}

#[tokio::test]
async fn tpcds_logical_q68() -> Result<()> {
    create_logical_plan(68).await
}

#[tokio::test]
async fn tpcds_logical_q69() -> Result<()> {
    create_logical_plan(69).await
}

#[tokio::test]
async fn tpcds_logical_q70() -> Result<()> {
    create_logical_plan(70).await
}

#[tokio::test]
async fn tpcds_logical_q71() -> Result<()> {
    create_logical_plan(71).await
}

#[tokio::test]
async fn tpcds_logical_q72() -> Result<()> {
    create_logical_plan(72).await
}

#[tokio::test]
async fn tpcds_logical_q73() -> Result<()> {
    create_logical_plan(73).await
}

#[tokio::test]
async fn tpcds_logical_q74() -> Result<()> {
    create_logical_plan(74).await
}

#[tokio::test]
async fn tpcds_logical_q75() -> Result<()> {
    create_logical_plan(75).await
}

#[tokio::test]
async fn tpcds_logical_q76() -> Result<()> {
    create_logical_plan(76).await
}

#[tokio::test]
async fn tpcds_logical_q77() -> Result<()> {
    create_logical_plan(77).await
}

#[tokio::test]
async fn tpcds_logical_q78() -> Result<()> {
    create_logical_plan(78).await
}

#[tokio::test]
async fn tpcds_logical_q79() -> Result<()> {
    create_logical_plan(79).await
}

#[tokio::test]
async fn tpcds_logical_q80() -> Result<()> {
    create_logical_plan(80).await
}

#[tokio::test]
async fn tpcds_logical_q81() -> Result<()> {
    create_logical_plan(81).await
}

#[tokio::test]
async fn tpcds_logical_q82() -> Result<()> {
    create_logical_plan(82).await
}

#[tokio::test]
async fn tpcds_logical_q83() -> Result<()> {
    create_logical_plan(83).await
}

#[tokio::test]
async fn tpcds_logical_q84() -> Result<()> {
    create_logical_plan(84).await
}

#[tokio::test]
async fn tpcds_logical_q85() -> Result<()> {
    create_logical_plan(85).await
}

#[tokio::test]
async fn tpcds_logical_q86() -> Result<()> {
    create_logical_plan(86).await
}

#[tokio::test]
async fn tpcds_logical_q87() -> Result<()> {
    create_logical_plan(87).await
}

#[tokio::test]
async fn tpcds_logical_q88() -> Result<()> {
    create_logical_plan(88).await
}

#[tokio::test]
async fn tpcds_logical_q89() -> Result<()> {
    create_logical_plan(89).await
}

#[tokio::test]
async fn tpcds_logical_q90() -> Result<()> {
    create_logical_plan(90).await
}

#[tokio::test]
async fn tpcds_logical_q91() -> Result<()> {
    create_logical_plan(91).await
}

#[tokio::test]
async fn tpcds_logical_q92() -> Result<()> {
    create_logical_plan(92).await
}

#[tokio::test]
async fn tpcds_logical_q93() -> Result<()> {
    create_logical_plan(93).await
}

#[tokio::test]
async fn tpcds_logical_q94() -> Result<()> {
    create_logical_plan(94).await
}

#[tokio::test]
async fn tpcds_logical_q95() -> Result<()> {
    create_logical_plan(95).await
}

#[tokio::test]
async fn tpcds_logical_q96() -> Result<()> {
    create_logical_plan(96).await
}

#[tokio::test]
async fn tpcds_logical_q97() -> Result<()> {
    create_logical_plan(97).await
}

#[tokio::test]
async fn tpcds_logical_q98() -> Result<()> {
    create_logical_plan(98).await
}

#[tokio::test]
async fn tpcds_logical_q99() -> Result<()> {
    create_logical_plan(99).await
}

#[tokio::test]
async fn tpcds_physical_q1() -> Result<()> {
    create_physical_plan(1).await
}

#[tokio::test]
async fn tpcds_physical_q2() -> Result<()> {
    create_physical_plan(2).await
}

#[tokio::test]
async fn tpcds_physical_q3() -> Result<()> {
    create_physical_plan(3).await
}

#[tokio::test]
async fn tpcds_physical_q4() -> Result<()> {
    create_physical_plan(4).await
}

#[tokio::test]
async fn tpcds_physical_q5() -> Result<()> {
    create_physical_plan(5).await
}

#[ignore] // Physical plan does not support logical expression (<subquery>)
#[tokio::test]
async fn tpcds_physical_q6() -> Result<()> {
    create_physical_plan(6).await
}

#[tokio::test]
async fn tpcds_physical_q7() -> Result<()> {
    create_physical_plan(7).await
}

#[ignore] // The type of Int32 = Int64 of binary physical should be same
#[tokio::test]
async fn tpcds_physical_q8() -> Result<()> {
    create_physical_plan(8).await
}

#[ignore] // Physical plan does not support logical expression (<subquery>)
#[tokio::test]
async fn tpcds_physical_q9() -> Result<()> {
    create_physical_plan(9).await
}

#[ignore] // FieldNotFound
#[tokio::test]
async fn tpcds_physical_q10() -> Result<()> {
    create_physical_plan(10).await
}

#[tokio::test]
async fn tpcds_physical_q11() -> Result<()> {
    create_physical_plan(11).await
}

#[tokio::test]
async fn tpcds_physical_q12() -> Result<()> {
    create_physical_plan(12).await
}

#[tokio::test]
async fn tpcds_physical_q13() -> Result<()> {
    create_physical_plan(13).await
}

#[ignore] // Physical plan does not support logical expression (<subquery>)
#[tokio::test]
async fn tpcds_physical_q14() -> Result<()> {
    create_physical_plan(14).await
}

#[tokio::test]
async fn tpcds_physical_q15() -> Result<()> {
    create_physical_plan(15).await
}

#[tokio::test]
async fn tpcds_physical_q16() -> Result<()> {
    create_physical_plan(16).await
}

#[tokio::test]
async fn tpcds_physical_q17() -> Result<()> {
    create_physical_plan(17).await
}

#[tokio::test]
async fn tpcds_physical_q18() -> Result<()> {
    create_physical_plan(18).await
}

#[tokio::test]
async fn tpcds_physical_q19() -> Result<()> {
    create_physical_plan(19).await
}

#[tokio::test]
async fn tpcds_physical_q20() -> Result<()> {
    create_physical_plan(20).await
}

#[tokio::test]
async fn tpcds_physical_q21() -> Result<()> {
    create_physical_plan(21).await
}

#[tokio::test]
async fn tpcds_physical_q22() -> Result<()> {
    create_physical_plan(22).await
}

#[ignore] // Physical plan does not support logical expression (<subquery>)
#[tokio::test]
async fn tpcds_physical_q23() -> Result<()> {
    create_physical_plan(23).await
}

#[tokio::test]
async fn tpcds_physical_q24() -> Result<()> {
    create_physical_plan(24).await
}

#[tokio::test]
async fn tpcds_physical_q25() -> Result<()> {
    create_physical_plan(25).await
}

#[tokio::test]
async fn tpcds_physical_q26() -> Result<()> {
    create_physical_plan(26).await
}

#[tokio::test]
async fn tpcds_physical_q27() -> Result<()> {
    create_physical_plan(27).await
}

#[tokio::test]
async fn tpcds_physical_q28() -> Result<()> {
    create_physical_plan(28).await
}

#[tokio::test]
async fn tpcds_physical_q29() -> Result<()> {
    create_physical_plan(29).await
}

#[tokio::test]
async fn tpcds_physical_q30() -> Result<()> {
    create_physical_plan(30).await
}

#[tokio::test]
async fn tpcds_physical_q31() -> Result<()> {
    create_physical_plan(31).await
}

#[tokio::test]
async fn tpcds_physical_q32() -> Result<()> {
    create_physical_plan(32).await
}

#[tokio::test]
async fn tpcds_physical_q33() -> Result<()> {
    create_physical_plan(33).await
}

#[tokio::test]
async fn tpcds_physical_q34() -> Result<()> {
    create_physical_plan(34).await
}

#[ignore] // FieldNotFound
#[tokio::test]
async fn tpcds_physical_q35() -> Result<()> {
    create_physical_plan(35).await
}

#[tokio::test]
async fn tpcds_physical_q36() -> Result<()> {
    create_physical_plan(36).await
}

#[tokio::test]
async fn tpcds_physical_q37() -> Result<()> {
    create_physical_plan(37).await
}

#[tokio::test]
async fn tpcds_physical_q38() -> Result<()> {
    create_physical_plan(38).await
}

#[tokio::test]
async fn tpcds_physical_q39() -> Result<()> {
    create_physical_plan(39).await
}

#[tokio::test]
async fn tpcds_physical_q40() -> Result<()> {
    create_physical_plan(40).await
}

#[ignore] // Physical plan does not support logical expression (<subquery>)
#[tokio::test]
async fn tpcds_physical_q41() -> Result<()> {
    create_physical_plan(41).await
}

#[tokio::test]
async fn tpcds_physical_q42() -> Result<()> {
    create_physical_plan(42).await
}

#[tokio::test]
async fn tpcds_physical_q43() -> Result<()> {
    create_physical_plan(43).await
}

#[ignore] // Physical plan does not support logical expression (<subquery>)
#[tokio::test]
async fn tpcds_physical_q44() -> Result<()> {
    create_physical_plan(44).await
}

#[ignore] // Physical plan does not support logical expression (<subquery>)
#[tokio::test]
async fn tpcds_physical_q45() -> Result<()> {
    create_physical_plan(45).await
}

#[tokio::test]
async fn tpcds_physical_q46() -> Result<()> {
    create_physical_plan(46).await
}

#[tokio::test]
async fn tpcds_physical_q47() -> Result<()> {
    create_physical_plan(47).await
}

#[tokio::test]
async fn tpcds_physical_q48() -> Result<()> {
    create_physical_plan(48).await
}

#[tokio::test]
async fn tpcds_physical_q49() -> Result<()> {
    create_physical_plan(49).await
}

#[tokio::test]
async fn tpcds_physical_q50() -> Result<()> {
    create_physical_plan(50).await
}

#[tokio::test]
async fn tpcds_physical_q51() -> Result<()> {
    create_physical_plan(51).await
}

#[tokio::test]
async fn tpcds_physical_q52() -> Result<()> {
    create_physical_plan(52).await
}

#[tokio::test]
async fn tpcds_physical_q53() -> Result<()> {
    create_physical_plan(53).await
}

#[ignore] // Physical plan does not support logical expression (<subquery>)
#[tokio::test]
async fn tpcds_physical_q54() -> Result<()> {
    create_physical_plan(54).await
}

#[tokio::test]
async fn tpcds_physical_q55() -> Result<()> {
    create_physical_plan(55).await
}

#[tokio::test]
async fn tpcds_physical_q56() -> Result<()> {
    create_physical_plan(56).await
}

#[tokio::test]
async fn tpcds_physical_q57() -> Result<()> {
    create_physical_plan(57).await
}

#[ignore] // Physical plan does not support logical expression (<subquery>)
#[tokio::test]
async fn tpcds_physical_q58() -> Result<()> {
    create_physical_plan(58).await
}

#[tokio::test]
async fn tpcds_physical_q59() -> Result<()> {
    create_physical_plan(59).await
}

#[tokio::test]
async fn tpcds_physical_q60() -> Result<()> {
    create_physical_plan(60).await
}

#[tokio::test]
async fn tpcds_physical_q61() -> Result<()> {
    create_physical_plan(61).await
}

#[tokio::test]
async fn tpcds_physical_q62() -> Result<()> {
    create_physical_plan(62).await
}

#[tokio::test]
async fn tpcds_physical_q63() -> Result<()> {
    create_physical_plan(63).await
}

#[ignore] // thread 'q64' has overflowed its stack
#[tokio::test]
async fn tpcds_physical_q64() -> Result<()> {
    create_physical_plan(64).await
}

#[tokio::test]
async fn tpcds_physical_q65() -> Result<()> {
    create_physical_plan(65).await
}

#[tokio::test]
async fn tpcds_physical_q66() -> Result<()> {
    create_physical_plan(66).await
}

#[tokio::test]
async fn tpcds_physical_q67() -> Result<()> {
    create_physical_plan(67).await
}

#[tokio::test]
async fn tpcds_physical_q68() -> Result<()> {
    create_physical_plan(68).await
}

#[tokio::test]
async fn tpcds_physical_q69() -> Result<()> {
    create_physical_plan(69).await
}

#[tokio::test]
async fn tpcds_physical_q70() -> Result<()> {
    create_physical_plan(70).await
}

#[tokio::test]
async fn tpcds_physical_q71() -> Result<()> {
    create_physical_plan(71).await
}

#[tokio::test]
async fn tpcds_physical_q72() -> Result<()> {
    create_physical_plan(72).await
}

#[tokio::test]
async fn tpcds_physical_q73() -> Result<()> {
    create_physical_plan(73).await
}

#[tokio::test]
async fn tpcds_physical_q74() -> Result<()> {
    create_physical_plan(74).await
}

#[tokio::test]
async fn tpcds_physical_q75() -> Result<()> {
    create_physical_plan(75).await
}

#[tokio::test]
async fn tpcds_physical_q76() -> Result<()> {
    create_physical_plan(76).await
}

#[tokio::test]
async fn tpcds_physical_q77() -> Result<()> {
    create_physical_plan(77).await
}

#[tokio::test]
async fn tpcds_physical_q78() -> Result<()> {
    create_physical_plan(78).await
}

#[tokio::test]
async fn tpcds_physical_q79() -> Result<()> {
    create_physical_plan(79).await
}

#[tokio::test]
async fn tpcds_physical_q80() -> Result<()> {
    create_physical_plan(80).await
}

#[tokio::test]
async fn tpcds_physical_q81() -> Result<()> {
    create_physical_plan(81).await
}

#[tokio::test]
async fn tpcds_physical_q82() -> Result<()> {
    create_physical_plan(82).await
}

#[tokio::test]
async fn tpcds_physical_q83() -> Result<()> {
    create_physical_plan(83).await
}

#[tokio::test]
async fn tpcds_physical_q84() -> Result<()> {
    create_physical_plan(84).await
}

#[ignore] // Physical plan does not support logical expression (<subquery>)
#[tokio::test]
async fn tpcds_physical_q85() -> Result<()> {
    create_physical_plan(85).await
}

#[tokio::test]
async fn tpcds_physical_q86() -> Result<()> {
    create_physical_plan(86).await
}

#[tokio::test]
async fn tpcds_physical_q87() -> Result<()> {
    create_physical_plan(87).await
}

#[tokio::test]
async fn tpcds_physical_q88() -> Result<()> {
    create_physical_plan(88).await
}

#[tokio::test]
async fn tpcds_physical_q89() -> Result<()> {
    create_physical_plan(89).await
}

#[tokio::test]
async fn tpcds_physical_q90() -> Result<()> {
    create_physical_plan(90).await
}

#[tokio::test]
async fn tpcds_physical_q91() -> Result<()> {
    create_physical_plan(91).await
}

#[tokio::test]
async fn tpcds_physical_q92() -> Result<()> {
    create_physical_plan(92).await
}

#[tokio::test]
async fn tpcds_physical_q93() -> Result<()> {
    create_physical_plan(93).await
}

#[tokio::test]
async fn tpcds_physical_q94() -> Result<()> {
    create_physical_plan(94).await
}

#[tokio::test]
async fn tpcds_physical_q95() -> Result<()> {
    create_physical_plan(95).await
}

#[tokio::test]
async fn tpcds_physical_q96() -> Result<()> {
    create_physical_plan(96).await
}

#[tokio::test]
async fn tpcds_physical_q97() -> Result<()> {
    create_physical_plan(97).await
}

#[tokio::test]
async fn tpcds_physical_q98() -> Result<()> {
    create_physical_plan(98).await
}

#[tokio::test]
async fn tpcds_physical_q99() -> Result<()> {
    create_physical_plan(99).await
}

async fn create_logical_plan(query_no: u8) -> Result<()> {
    regression_test(query_no, false).await
}

async fn create_physical_plan(query_no: u8) -> Result<()> {
    regression_test(query_no, true).await
}

async fn regression_test(query_no: u8, create_physical: bool) -> Result<()> {
    let filename = format!("tests/tpc-ds/{query_no}.sql");
    let sql = fs::read_to_string(filename).expect("Could not read query");

    let config = SessionConfig::default();
    let ctx = SessionContext::with_config(config);
    let tables = get_table_definitions();
    for table in &tables {
        ctx.register_table(
            table.name.as_str(),
            Arc::new(MemTable::try_new(Arc::new(table.schema.clone()), vec![])?),
        )?;
    }

    // some queries have multiple statements
    let sql = sql
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .collect::<Vec<_>>();

    for sql in &sql {
        let df = ctx.sql(sql).await?;
        let (state, plan) = df.into_parts();
        let plan = state.optimize(&plan)?;
        if create_physical {
            let _ = state.create_physical_plan(&plan).await?;
        }
    }

    Ok(())
}

pub struct TableDef {
    pub name: String,
    pub schema: Schema,
}

impl TableDef {
    fn new(name: &str, schema: Schema) -> Self {
        Self {
            name: name.to_string(),
            schema,
        }
    }
}

pub fn get_table_definitions() -> Vec<TableDef> {
    vec![
        TableDef::new(
            "catalog_sales",
            Schema::new(vec![
                Field::new("cs_sold_date_sk", DataType::Int32, false),
                Field::new("cs_sold_time_sk", DataType::Int32, false),
                Field::new("cs_ship_date_sk", DataType::Int32, false),
                Field::new("cs_bill_customer_sk", DataType::Int32, false),
                Field::new("cs_bill_cdemo_sk", DataType::Int32, false),
                Field::new("cs_bill_hdemo_sk", DataType::Int32, false),
                Field::new("cs_bill_addr_sk", DataType::Int32, false),
                Field::new("cs_ship_customer_sk", DataType::Int32, false),
                Field::new("cs_ship_cdemo_sk", DataType::Int32, false),
                Field::new("cs_ship_hdemo_sk", DataType::Int32, false),
                Field::new("cs_ship_addr_sk", DataType::Int32, false),
                Field::new("cs_call_center_sk", DataType::Int32, false),
                Field::new("cs_catalog_page_sk", DataType::Int32, false),
                Field::new("cs_ship_mode_sk", DataType::Int32, false),
                Field::new("cs_warehouse_sk", DataType::Int32, false),
                Field::new("cs_item_sk", DataType::Int32, false),
                Field::new("cs_promo_sk", DataType::Int32, false),
                Field::new("cs_order_number", DataType::Int64, false),
                Field::new("cs_quantity", DataType::Int32, false),
                Field::new("cs_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("cs_list_price", DataType::Decimal128(7, 2), false),
                Field::new("cs_sales_price", DataType::Decimal128(7, 2), false),
                Field::new("cs_ext_discount_amt", DataType::Decimal128(7, 2), false),
                Field::new("cs_ext_sales_price", DataType::Decimal128(7, 2), false),
                Field::new("cs_ext_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("cs_ext_list_price", DataType::Decimal128(7, 2), false),
                Field::new("cs_ext_tax", DataType::Decimal128(7, 2), false),
                Field::new("cs_coupon_amt", DataType::Decimal128(7, 2), false),
                Field::new("cs_ext_ship_cost", DataType::Decimal128(7, 2), false),
                Field::new("cs_net_paid", DataType::Decimal128(7, 2), false),
                Field::new("cs_net_paid_inc_tax", DataType::Decimal128(7, 2), false),
                Field::new("cs_net_paid_inc_ship", DataType::Decimal128(7, 2), false),
                Field::new(
                    "cs_net_paid_inc_ship_tax",
                    DataType::Decimal128(7, 2),
                    false,
                ),
                Field::new("cs_net_profit", DataType::Decimal128(7, 2), false),
            ]),
        ),
        TableDef::new(
            "catalog_returns",
            Schema::new(vec![
                Field::new("cr_returned_date_sk", DataType::Int32, false),
                Field::new("cr_returned_time_sk", DataType::Int32, false),
                Field::new("cr_item_sk", DataType::Int32, false),
                Field::new("cr_refunded_customer_sk", DataType::Int32, false),
                Field::new("cr_refunded_cdemo_sk", DataType::Int32, false),
                Field::new("cr_refunded_hdemo_sk", DataType::Int32, false),
                Field::new("cr_refunded_addr_sk", DataType::Int32, false),
                Field::new("cr_returning_customer_sk", DataType::Int32, false),
                Field::new("cr_returning_cdemo_sk", DataType::Int32, false),
                Field::new("cr_returning_hdemo_sk", DataType::Int32, false),
                Field::new("cr_returning_addr_sk", DataType::Int32, false),
                Field::new("cr_call_center_sk", DataType::Int32, false),
                Field::new("cr_catalog_page_sk", DataType::Int32, false),
                Field::new("cr_ship_mode_sk", DataType::Int32, false),
                Field::new("cr_warehouse_sk", DataType::Int32, false),
                Field::new("cr_reason_sk", DataType::Int32, false),
                Field::new("cr_order_number", DataType::Int64, false),
                Field::new("cr_return_quantity", DataType::Int32, false),
                Field::new("cr_return_amount", DataType::Decimal128(7, 2), false),
                Field::new("cr_return_tax", DataType::Decimal128(7, 2), false),
                Field::new("cr_return_amt_inc_tax", DataType::Decimal128(7, 2), false),
                Field::new("cr_fee", DataType::Decimal128(7, 2), false),
                Field::new("cr_return_ship_cost", DataType::Decimal128(7, 2), false),
                Field::new("cr_refunded_cash", DataType::Decimal128(7, 2), false),
                Field::new("cr_reversed_charge", DataType::Decimal128(7, 2), false),
                Field::new("cr_store_credit", DataType::Decimal128(7, 2), false),
                Field::new("cr_net_loss", DataType::Decimal128(7, 2), false),
            ]),
        ),
        TableDef::new(
            "inventory",
            Schema::new(vec![
                Field::new("inv_date_sk", DataType::Int32, false),
                Field::new("inv_item_sk", DataType::Int32, false),
                Field::new("inv_warehouse_sk", DataType::Int32, false),
                Field::new("inv_quantity_on_hand", DataType::Int32, false),
            ]),
        ),
        TableDef::new(
            "store_sales",
            Schema::new(vec![
                Field::new("ss_sold_date_sk", DataType::Int32, false),
                Field::new("ss_sold_time_sk", DataType::Int32, false),
                Field::new("ss_item_sk", DataType::Int32, false),
                Field::new("ss_customer_sk", DataType::Int32, false),
                Field::new("ss_cdemo_sk", DataType::Int32, false),
                Field::new("ss_hdemo_sk", DataType::Int32, false),
                Field::new("ss_addr_sk", DataType::Int32, false),
                Field::new("ss_store_sk", DataType::Int32, false),
                Field::new("ss_promo_sk", DataType::Int32, false),
                Field::new("ss_ticket_number", DataType::Int64, false),
                Field::new("ss_quantity", DataType::Int32, false),
                Field::new("ss_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("ss_list_price", DataType::Decimal128(7, 2), false),
                Field::new("ss_sales_price", DataType::Decimal128(7, 2), false),
                Field::new("ss_ext_discount_amt", DataType::Decimal128(7, 2), false),
                Field::new("ss_ext_sales_price", DataType::Decimal128(7, 2), false),
                Field::new("ss_ext_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("ss_ext_list_price", DataType::Decimal128(7, 2), false),
                Field::new("ss_ext_tax", DataType::Decimal128(7, 2), false),
                Field::new("ss_coupon_amt", DataType::Decimal128(7, 2), false),
                Field::new("ss_net_paid", DataType::Decimal128(7, 2), false),
                Field::new("ss_net_paid_inc_tax", DataType::Decimal128(7, 2), false),
                Field::new("ss_net_profit", DataType::Decimal128(7, 2), false),
            ]),
        ),
        TableDef::new(
            "store_returns",
            Schema::new(vec![
                Field::new("sr_returned_date_sk", DataType::Int32, false),
                Field::new("sr_return_time_sk", DataType::Int32, false),
                Field::new("sr_item_sk", DataType::Int32, false),
                Field::new("sr_customer_sk", DataType::Int32, false),
                Field::new("sr_cdemo_sk", DataType::Int32, false),
                Field::new("sr_hdemo_sk", DataType::Int32, false),
                Field::new("sr_addr_sk", DataType::Int32, false),
                Field::new("sr_store_sk", DataType::Int32, false),
                Field::new("sr_reason_sk", DataType::Int32, false),
                Field::new("sr_ticket_number", DataType::Int64, false),
                Field::new("sr_return_quantity", DataType::Int32, false),
                Field::new("sr_return_amt", DataType::Decimal128(7, 2), false),
                Field::new("sr_return_tax", DataType::Decimal128(7, 2), false),
                Field::new("sr_return_amt_inc_tax", DataType::Decimal128(7, 2), false),
                Field::new("sr_fee", DataType::Decimal128(7, 2), false),
                Field::new("sr_return_ship_cost", DataType::Decimal128(7, 2), false),
                Field::new("sr_refunded_cash", DataType::Decimal128(7, 2), false),
                Field::new("sr_reversed_charge", DataType::Decimal128(7, 2), false),
                Field::new("sr_store_credit", DataType::Decimal128(7, 2), false),
                Field::new("sr_net_loss", DataType::Decimal128(7, 2), false),
            ]),
        ),
        TableDef::new(
            "web_sales",
            Schema::new(vec![
                Field::new("ws_sold_date_sk", DataType::Int32, false),
                Field::new("ws_sold_time_sk", DataType::Int32, false),
                Field::new("ws_ship_date_sk", DataType::Int32, false),
                Field::new("ws_item_sk", DataType::Int32, false),
                Field::new("ws_bill_customer_sk", DataType::Int32, false),
                Field::new("ws_bill_cdemo_sk", DataType::Int32, false),
                Field::new("ws_bill_hdemo_sk", DataType::Int32, false),
                Field::new("ws_bill_addr_sk", DataType::Int32, false),
                Field::new("ws_ship_customer_sk", DataType::Int32, false),
                Field::new("ws_ship_cdemo_sk", DataType::Int32, false),
                Field::new("ws_ship_hdemo_sk", DataType::Int32, false),
                Field::new("ws_ship_addr_sk", DataType::Int32, false),
                Field::new("ws_web_page_sk", DataType::Int32, false),
                Field::new("ws_web_site_sk", DataType::Int32, false),
                Field::new("ws_ship_mode_sk", DataType::Int32, false),
                Field::new("ws_warehouse_sk", DataType::Int32, false),
                Field::new("ws_promo_sk", DataType::Int32, false),
                Field::new("ws_order_number", DataType::Int64, false),
                Field::new("ws_quantity", DataType::Int32, false),
                Field::new("ws_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("ws_list_price", DataType::Decimal128(7, 2), false),
                Field::new("ws_sales_price", DataType::Decimal128(7, 2), false),
                Field::new("ws_ext_discount_amt", DataType::Decimal128(7, 2), false),
                Field::new("ws_ext_sales_price", DataType::Decimal128(7, 2), false),
                Field::new("ws_ext_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("ws_ext_list_price", DataType::Decimal128(7, 2), false),
                Field::new("ws_ext_tax", DataType::Decimal128(7, 2), false),
                Field::new("ws_coupon_amt", DataType::Decimal128(7, 2), false),
                Field::new("ws_ext_ship_cost", DataType::Decimal128(7, 2), false),
                Field::new("ws_net_paid", DataType::Decimal128(7, 2), false),
                Field::new("ws_net_paid_inc_tax", DataType::Decimal128(7, 2), false),
                Field::new("ws_net_paid_inc_ship", DataType::Decimal128(7, 2), false),
                Field::new(
                    "ws_net_paid_inc_ship_tax",
                    DataType::Decimal128(7, 2),
                    false,
                ),
                Field::new("ws_net_profit", DataType::Decimal128(7, 2), false),
            ]),
        ),
        TableDef::new(
            "web_returns",
            Schema::new(vec![
                Field::new("wr_returned_date_sk", DataType::Int32, false),
                Field::new("wr_returned_time_sk", DataType::Int32, false),
                Field::new("wr_item_sk", DataType::Int32, false),
                Field::new("wr_refunded_customer_sk", DataType::Int32, false),
                Field::new("wr_refunded_cdemo_sk", DataType::Int32, false),
                Field::new("wr_refunded_hdemo_sk", DataType::Int32, false),
                Field::new("wr_refunded_addr_sk", DataType::Int32, false),
                Field::new("wr_returning_customer_sk", DataType::Int32, false),
                Field::new("wr_returning_cdemo_sk", DataType::Int32, false),
                Field::new("wr_returning_hdemo_sk", DataType::Int32, false),
                Field::new("wr_returning_addr_sk", DataType::Int32, false),
                Field::new("wr_web_page_sk", DataType::Int32, false),
                Field::new("wr_reason_sk", DataType::Int32, false),
                Field::new("wr_order_number", DataType::Int64, false),
                Field::new("wr_return_quantity", DataType::Int32, false),
                Field::new("wr_return_amt", DataType::Decimal128(7, 2), false),
                Field::new("wr_return_tax", DataType::Decimal128(7, 2), false),
                Field::new("wr_return_amt_inc_tax", DataType::Decimal128(7, 2), false),
                Field::new("wr_fee", DataType::Decimal128(7, 2), false),
                Field::new("wr_return_ship_cost", DataType::Decimal128(7, 2), false),
                Field::new("wr_refunded_cash", DataType::Decimal128(7, 2), false),
                Field::new("wr_reversed_charge", DataType::Decimal128(7, 2), false),
                Field::new("wr_account_credit", DataType::Decimal128(7, 2), false),
                Field::new("wr_net_loss", DataType::Decimal128(7, 2), false),
            ]),
        ),
        TableDef::new(
            "call_center",
            Schema::new(vec![
                Field::new("cc_call_center_sk", DataType::Int32, false),
                Field::new("cc_call_center_id", DataType::Utf8, false),
                Field::new("cc_rec_start_date", DataType::Date32, false),
                Field::new("cc_rec_end_date", DataType::Date32, false),
                Field::new("cc_closed_date_sk", DataType::Int32, false),
                Field::new("cc_open_date_sk", DataType::Int32, false),
                Field::new("cc_name", DataType::Utf8, false),
                Field::new("cc_class", DataType::Utf8, false),
                Field::new("cc_employees", DataType::Int32, false),
                Field::new("cc_sq_ft", DataType::Int32, false),
                Field::new("cc_hours", DataType::Utf8, false),
                Field::new("cc_manager", DataType::Utf8, false),
                Field::new("cc_mkt_id", DataType::Int32, false),
                Field::new("cc_mkt_class", DataType::Utf8, false),
                Field::new("cc_mkt_desc", DataType::Utf8, false),
                Field::new("cc_market_manager", DataType::Utf8, false),
                Field::new("cc_division", DataType::Int32, false),
                Field::new("cc_division_name", DataType::Utf8, false),
                Field::new("cc_company", DataType::Int32, false),
                Field::new("cc_company_name", DataType::Utf8, false),
                Field::new("cc_street_number", DataType::Utf8, false),
                Field::new("cc_street_name", DataType::Utf8, false),
                Field::new("cc_street_type", DataType::Utf8, false),
                Field::new("cc_suite_number", DataType::Utf8, false),
                Field::new("cc_city", DataType::Utf8, false),
                Field::new("cc_county", DataType::Utf8, false),
                Field::new("cc_state", DataType::Utf8, false),
                Field::new("cc_zip", DataType::Utf8, false),
                Field::new("cc_country", DataType::Utf8, false),
                Field::new("cc_gmt_offset", DataType::Decimal128(5, 2), false),
                Field::new("cc_tax_percentage", DataType::Decimal128(5, 2), false),
            ]),
        ),
        TableDef::new(
            "catalog_page",
            Schema::new(vec![
                Field::new("cp_catalog_page_sk", DataType::Int32, false),
                Field::new("cp_catalog_page_id", DataType::Utf8, false),
                Field::new("cp_start_date_sk", DataType::Int32, false),
                Field::new("cp_end_date_sk", DataType::Int32, false),
                Field::new("cp_department", DataType::Utf8, false),
                Field::new("cp_catalog_number", DataType::Int32, false),
                Field::new("cp_catalog_page_number", DataType::Int32, false),
                Field::new("cp_description", DataType::Utf8, false),
                Field::new("cp_type", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "customer",
            Schema::new(vec![
                Field::new("c_customer_sk", DataType::Int32, false),
                Field::new("c_customer_id", DataType::Utf8, false),
                Field::new("c_current_cdemo_sk", DataType::Int32, false),
                Field::new("c_current_hdemo_sk", DataType::Int32, false),
                Field::new("c_current_addr_sk", DataType::Int32, false),
                Field::new("c_first_shipto_date_sk", DataType::Int32, false),
                Field::new("c_first_sales_date_sk", DataType::Int32, false),
                Field::new("c_salutation", DataType::Utf8, false),
                Field::new("c_first_name", DataType::Utf8, false),
                Field::new("c_last_name", DataType::Utf8, false),
                Field::new("c_preferred_cust_flag", DataType::Utf8, false),
                Field::new("c_birth_day", DataType::Int32, false),
                Field::new("c_birth_month", DataType::Int32, false),
                Field::new("c_birth_year", DataType::Int32, false),
                Field::new("c_birth_country", DataType::Utf8, false),
                Field::new("c_login", DataType::Utf8, false),
                Field::new("c_email_address", DataType::Utf8, false),
                Field::new("c_last_review_date_sk", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "customer_address",
            Schema::new(vec![
                Field::new("ca_address_sk", DataType::Int32, false),
                Field::new("ca_address_id", DataType::Utf8, false),
                Field::new("ca_street_number", DataType::Utf8, false),
                Field::new("ca_street_name", DataType::Utf8, false),
                Field::new("ca_street_type", DataType::Utf8, false),
                Field::new("ca_suite_number", DataType::Utf8, false),
                Field::new("ca_city", DataType::Utf8, false),
                Field::new("ca_county", DataType::Utf8, false),
                Field::new("ca_state", DataType::Utf8, false),
                Field::new("ca_zip", DataType::Utf8, false),
                Field::new("ca_country", DataType::Utf8, false),
                Field::new("ca_gmt_offset", DataType::Decimal128(5, 2), false),
                Field::new("ca_location_type", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "customer_demographics",
            Schema::new(vec![
                Field::new("cd_demo_sk", DataType::Int32, false),
                Field::new("cd_gender", DataType::Utf8, false),
                Field::new("cd_marital_status", DataType::Utf8, false),
                Field::new("cd_education_status", DataType::Utf8, false),
                Field::new("cd_purchase_estimate", DataType::Int32, false),
                Field::new("cd_credit_rating", DataType::Utf8, false),
                Field::new("cd_dep_count", DataType::Int32, false),
                Field::new("cd_dep_employed_count", DataType::Int32, false),
                Field::new("cd_dep_college_count", DataType::Int32, false),
            ]),
        ),
        TableDef::new(
            "date_dim",
            Schema::new(vec![
                Field::new("d_date_sk", DataType::Int32, false),
                Field::new("d_date_id", DataType::Utf8, false),
                Field::new("d_date", DataType::Date32, false),
                Field::new("d_month_seq", DataType::Int32, false),
                Field::new("d_week_seq", DataType::Int32, false),
                Field::new("d_quarter_seq", DataType::Int32, false),
                Field::new("d_year", DataType::Int32, false),
                Field::new("d_dow", DataType::Int32, false),
                Field::new("d_moy", DataType::Int32, false),
                Field::new("d_dom", DataType::Int32, false),
                Field::new("d_qoy", DataType::Int32, false),
                Field::new("d_fy_year", DataType::Int32, false),
                Field::new("d_fy_quarter_seq", DataType::Int32, false),
                Field::new("d_fy_week_seq", DataType::Int32, false),
                Field::new("d_day_name", DataType::Utf8, false),
                Field::new("d_quarter_name", DataType::Utf8, false),
                Field::new("d_holiday", DataType::Utf8, false),
                Field::new("d_weekend", DataType::Utf8, false),
                Field::new("d_following_holiday", DataType::Utf8, false),
                Field::new("d_first_dom", DataType::Int32, false),
                Field::new("d_last_dom", DataType::Int32, false),
                Field::new("d_same_day_ly", DataType::Int32, false),
                Field::new("d_same_day_lq", DataType::Int32, false),
                Field::new("d_current_day", DataType::Utf8, false),
                Field::new("d_current_week", DataType::Utf8, false),
                Field::new("d_current_month", DataType::Utf8, false),
                Field::new("d_current_quarter", DataType::Utf8, false),
                Field::new("d_current_year", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "household_demographics",
            Schema::new(vec![
                Field::new("hd_demo_sk", DataType::Int32, false),
                Field::new("hd_income_band_sk", DataType::Int32, false),
                Field::new("hd_buy_potential", DataType::Utf8, false),
                Field::new("hd_dep_count", DataType::Int32, false),
                Field::new("hd_vehicle_count", DataType::Int32, false),
            ]),
        ),
        TableDef::new(
            "income_band",
            Schema::new(vec![
                Field::new("ib_income_band_sk", DataType::Int32, false),
                Field::new("ib_lower_bound", DataType::Int32, false),
                Field::new("ib_upper_bound", DataType::Int32, false),
            ]),
        ),
        TableDef::new(
            "item",
            Schema::new(vec![
                Field::new("i_item_sk", DataType::Int32, false),
                Field::new("i_item_id", DataType::Utf8, false),
                Field::new("i_rec_start_date", DataType::Date32, false),
                Field::new("i_rec_end_date", DataType::Date32, false),
                Field::new("i_item_desc", DataType::Utf8, false),
                Field::new("i_current_price", DataType::Decimal128(7, 2), false),
                Field::new("i_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("i_brand_id", DataType::Int32, false),
                Field::new("i_brand", DataType::Utf8, false),
                Field::new("i_class_id", DataType::Int32, false),
                Field::new("i_class", DataType::Utf8, false),
                Field::new("i_category_id", DataType::Int32, false),
                Field::new("i_category", DataType::Utf8, false),
                Field::new("i_manufact_id", DataType::Int32, false),
                Field::new("i_manufact", DataType::Utf8, false),
                Field::new("i_size", DataType::Utf8, false),
                Field::new("i_formulation", DataType::Utf8, false),
                Field::new("i_color", DataType::Utf8, false),
                Field::new("i_units", DataType::Utf8, false),
                Field::new("i_container", DataType::Utf8, false),
                Field::new("i_manager_id", DataType::Int32, false),
                Field::new("i_product_name", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "promotion",
            Schema::new(vec![
                Field::new("p_promo_sk", DataType::Int32, false),
                Field::new("p_promo_id", DataType::Utf8, false),
                Field::new("p_start_date_sk", DataType::Int32, false),
                Field::new("p_end_date_sk", DataType::Int32, false),
                Field::new("p_item_sk", DataType::Int32, false),
                Field::new("p_cost", DataType::Decimal128(15, 2), false),
                Field::new("p_response_target", DataType::Int32, false),
                Field::new("p_promo_name", DataType::Utf8, false),
                Field::new("p_channel_dmail", DataType::Utf8, false),
                Field::new("p_channel_email", DataType::Utf8, false),
                Field::new("p_channel_catalog", DataType::Utf8, false),
                Field::new("p_channel_tv", DataType::Utf8, false),
                Field::new("p_channel_radio", DataType::Utf8, false),
                Field::new("p_channel_press", DataType::Utf8, false),
                Field::new("p_channel_event", DataType::Utf8, false),
                Field::new("p_channel_demo", DataType::Utf8, false),
                Field::new("p_channel_details", DataType::Utf8, false),
                Field::new("p_purpose", DataType::Utf8, false),
                Field::new("p_discount_active", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "reason",
            Schema::new(vec![
                Field::new("r_reason_sk", DataType::Int32, false),
                Field::new("r_reason_id", DataType::Utf8, false),
                Field::new("r_reason_desc", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "ship_mode",
            //),
            Schema::new(vec![
                Field::new("sm_ship_mode_sk", DataType::Int32, false),
                Field::new("sm_ship_mode_id", DataType::Utf8, false),
                Field::new("sm_type", DataType::Utf8, false),
                Field::new("sm_code", DataType::Utf8, false),
                Field::new("sm_carrier", DataType::Utf8, false),
                Field::new("sm_contract", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "store",
            Schema::new(vec![
                Field::new("s_store_sk", DataType::Int32, false),
                Field::new("s_store_id", DataType::Utf8, false),
                Field::new("s_rec_start_date", DataType::Date32, false),
                Field::new("s_rec_end_date", DataType::Date32, false),
                Field::new("s_closed_date_sk", DataType::Int32, false),
                Field::new("s_store_name", DataType::Utf8, false),
                Field::new("s_number_employees", DataType::Int32, false),
                Field::new("s_floor_space", DataType::Int32, false),
                Field::new("s_hours", DataType::Utf8, false),
                Field::new("s_manager", DataType::Utf8, false),
                Field::new("s_market_id", DataType::Int32, false),
                Field::new("s_geography_class", DataType::Utf8, false),
                Field::new("s_market_desc", DataType::Utf8, false),
                Field::new("s_market_manager", DataType::Utf8, false),
                Field::new("s_division_id", DataType::Int32, false),
                Field::new("s_division_name", DataType::Utf8, false),
                Field::new("s_company_id", DataType::Int32, false),
                Field::new("s_company_name", DataType::Utf8, false),
                Field::new("s_street_number", DataType::Utf8, false),
                Field::new("s_street_name", DataType::Utf8, false),
                Field::new("s_street_type", DataType::Utf8, false),
                Field::new("s_suite_number", DataType::Utf8, false),
                Field::new("s_city", DataType::Utf8, false),
                Field::new("s_county", DataType::Utf8, false),
                Field::new("s_state", DataType::Utf8, false),
                Field::new("s_zip", DataType::Utf8, false),
                Field::new("s_country", DataType::Utf8, false),
                Field::new("s_gmt_offset", DataType::Decimal128(5, 2), false),
                Field::new("s_tax_precentage", DataType::Decimal128(5, 2), false),
            ]),
        ),
        TableDef::new(
            "time_dim",
            Schema::new(vec![
                Field::new("t_time_sk", DataType::Int32, false),
                Field::new("t_time_id", DataType::Utf8, false),
                Field::new("t_time", DataType::Int32, false),
                Field::new("t_hour", DataType::Int32, false),
                Field::new("t_minute", DataType::Int32, false),
                Field::new("t_second", DataType::Int32, false),
                Field::new("t_am_pm", DataType::Utf8, false),
                Field::new("t_shift", DataType::Utf8, false),
                Field::new("t_sub_shift", DataType::Utf8, false),
                Field::new("t_meal_time", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "warehouse",
            //),
            Schema::new(vec![
                Field::new("w_warehouse_sk", DataType::Int32, false),
                Field::new("w_warehouse_id", DataType::Utf8, false),
                Field::new("w_warehouse_name", DataType::Utf8, false),
                Field::new("w_warehouse_sq_ft", DataType::Int32, false),
                Field::new("w_street_number", DataType::Utf8, false),
                Field::new("w_street_name", DataType::Utf8, false),
                Field::new("w_street_type", DataType::Utf8, false),
                Field::new("w_suite_number", DataType::Utf8, false),
                Field::new("w_city", DataType::Utf8, false),
                Field::new("w_county", DataType::Utf8, false),
                Field::new("w_state", DataType::Utf8, false),
                Field::new("w_zip", DataType::Utf8, false),
                Field::new("w_country", DataType::Utf8, false),
                Field::new("w_gmt_offset", DataType::Decimal128(5, 2), false),
            ]),
        ),
        TableDef::new(
            "web_page",
            Schema::new(vec![
                Field::new("wp_web_page_sk", DataType::Int32, false),
                Field::new("wp_web_page_id", DataType::Utf8, false),
                Field::new("wp_rec_start_date", DataType::Date32, false),
                Field::new("wp_rec_end_date", DataType::Date32, false),
                Field::new("wp_creation_date_sk", DataType::Int32, false),
                Field::new("wp_access_date_sk", DataType::Int32, false),
                Field::new("wp_autogen_flag", DataType::Utf8, false),
                Field::new("wp_customer_sk", DataType::Int32, false),
                Field::new("wp_url", DataType::Utf8, false),
                Field::new("wp_type", DataType::Utf8, false),
                Field::new("wp_char_count", DataType::Int32, false),
                Field::new("wp_link_count", DataType::Int32, false),
                Field::new("wp_image_count", DataType::Int32, false),
                Field::new("wp_max_ad_count", DataType::Int32, false),
            ]),
        ),
        TableDef::new(
            "web_site",
            Schema::new(vec![
                Field::new("web_site_sk", DataType::Int32, false),
                Field::new("web_site_id", DataType::Utf8, false),
                Field::new("web_rec_start_date", DataType::Date32, false),
                Field::new("web_rec_end_date", DataType::Date32, false),
                Field::new("web_name", DataType::Utf8, false),
                Field::new("web_open_date_sk", DataType::Int32, false),
                Field::new("web_close_date_sk", DataType::Int32, false),
                Field::new("web_class", DataType::Utf8, false),
                Field::new("web_manager", DataType::Utf8, false),
                Field::new("web_mkt_id", DataType::Int32, false),
                Field::new("web_mkt_class", DataType::Utf8, false),
                Field::new("web_mkt_desc", DataType::Utf8, false),
                Field::new("web_market_manager", DataType::Utf8, false),
                Field::new("web_company_id", DataType::Int32, false),
                Field::new("web_company_name", DataType::Utf8, false),
                Field::new("web_street_number", DataType::Utf8, false),
                Field::new("web_street_name", DataType::Utf8, false),
                Field::new("web_street_type", DataType::Utf8, false),
                Field::new("web_suite_number", DataType::Utf8, false),
                Field::new("web_city", DataType::Utf8, false),
                Field::new("web_county", DataType::Utf8, false),
                Field::new("web_state", DataType::Utf8, false),
                Field::new("web_zip", DataType::Utf8, false),
                Field::new("web_country", DataType::Utf8, false),
                Field::new("web_gmt_offset", DataType::Decimal128(5, 2), false),
                Field::new("web_tax_percentage", DataType::Decimal128(5, 2), false),
            ]),
        ),
    ]
}
