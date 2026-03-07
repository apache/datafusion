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

#![allow(clippy::absurd_extreme_comparisons)]
#![allow(unused_comparisons)]

use crate::statistics::StatsVsMetricsDisplayOptions;
use datafusion::common::Result;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_physical_plan::collect;
use std::fs;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statistics::Node;

    #[tokio::test]
    async fn tpcds_1() -> Result<()> {
        let result = tpcds_stats_vs_metrics(1).await?;
        assert!(result.row_estimation_accuracy >= 20);
        assert!(result.byte_estimation_accuracy >= 8);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_2() -> Result<()> {
        let result = tpcds_stats_vs_metrics(2).await?;
        assert!(result.row_estimation_accuracy >= 34);
        assert!(result.byte_estimation_accuracy >= 16);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_3() -> Result<()> {
        let result = tpcds_stats_vs_metrics(3).await?;
        assert!(result.row_estimation_accuracy >= 44);
        assert!(result.byte_estimation_accuracy >= 10);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_4() -> Result<()> {
        let result = tpcds_stats_vs_metrics(4).await?;
        assert!(result.row_estimation_accuracy >= 22);
        assert!(result.byte_estimation_accuracy >= 4);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_5() -> Result<()> {
        let result = tpcds_stats_vs_metrics(5).await?;
        assert!(result.row_estimation_accuracy >= 21);
        assert!(result.byte_estimation_accuracy >= 12);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_6() -> Result<()> {
        let result = tpcds_stats_vs_metrics(6).await?;
        assert!(result.row_estimation_accuracy >= 28);
        assert!(result.byte_estimation_accuracy >= 5);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_7() -> Result<()> {
        let result = tpcds_stats_vs_metrics(7).await?;
        assert!(result.row_estimation_accuracy >= 34);
        assert!(result.byte_estimation_accuracy >= 4);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_8() -> Result<()> {
        let result = tpcds_stats_vs_metrics(8).await?;
        assert!(result.row_estimation_accuracy >= 34);
        assert!(result.byte_estimation_accuracy >= 2);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_9() -> Result<()> {
        let result = tpcds_stats_vs_metrics(9).await?;
        assert!(result.row_estimation_accuracy >= 55);
        assert!(result.byte_estimation_accuracy >= 30);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_10() -> Result<()> {
        let result = tpcds_stats_vs_metrics(10).await?;
        assert!(result.row_estimation_accuracy >= 33);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_11() -> Result<()> {
        let result = tpcds_stats_vs_metrics(11).await?;
        assert!(result.row_estimation_accuracy >= 23);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_12() -> Result<()> {
        let result = tpcds_stats_vs_metrics(12).await?;
        assert!(result.row_estimation_accuracy >= 22);
        assert!(result.byte_estimation_accuracy >= 5);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_13() -> Result<()> {
        let result = tpcds_stats_vs_metrics(13).await?;
        assert!(result.row_estimation_accuracy >= 54);
        assert!(result.byte_estimation_accuracy >= 9);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_14() -> Result<()> {
        let result = tpcds_stats_vs_metrics(14).await?;
        assert!(result.row_estimation_accuracy >= 37);
        assert!(result.byte_estimation_accuracy >= 16);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_15() -> Result<()> {
        let result = tpcds_stats_vs_metrics(15).await?;
        assert!(result.row_estimation_accuracy >= 15);
        assert!(result.byte_estimation_accuracy >= 8);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_16() -> Result<()> {
        let result = tpcds_stats_vs_metrics(16).await?;
        assert!(result.row_estimation_accuracy >= 24);
        assert!(result.byte_estimation_accuracy >= 9);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_17() -> Result<()> {
        let result = tpcds_stats_vs_metrics(17).await?;
        assert!(result.row_estimation_accuracy >= 11);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_18() -> Result<()> {
        let result = tpcds_stats_vs_metrics(18).await?;
        assert!(result.row_estimation_accuracy >= 36);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_19() -> Result<()> {
        let result = tpcds_stats_vs_metrics(19).await?;
        assert!(result.row_estimation_accuracy >= 36);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_20() -> Result<()> {
        let result = tpcds_stats_vs_metrics(20).await?;
        assert!(result.row_estimation_accuracy >= 22);
        assert!(result.byte_estimation_accuracy >= 5);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_21() -> Result<()> {
        let result = tpcds_stats_vs_metrics(21).await?;
        assert!(result.row_estimation_accuracy >= 31);
        assert!(result.byte_estimation_accuracy >= 3);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_22() -> Result<()> {
        let result = tpcds_stats_vs_metrics(22).await?;
        assert!(result.row_estimation_accuracy >= 18);
        assert!(result.byte_estimation_accuracy >= 2);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_23() -> Result<()> {
        let result = tpcds_stats_vs_metrics(23).await?;
        assert!(result.row_estimation_accuracy >= 18);
        assert!(result.byte_estimation_accuracy >= 12);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_24() -> Result<()> {
        let result = tpcds_stats_vs_metrics(24).await?;
        assert!(result.row_estimation_accuracy >= 33);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_25() -> Result<()> {
        let result = tpcds_stats_vs_metrics(25).await?;
        assert!(result.row_estimation_accuracy >= 18);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_26() -> Result<()> {
        let result = tpcds_stats_vs_metrics(26).await?;
        assert!(result.row_estimation_accuracy >= 31);
        assert!(result.byte_estimation_accuracy >= 2);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_27() -> Result<()> {
        let result = tpcds_stats_vs_metrics(27).await?;
        assert!(result.row_estimation_accuracy >= 20);
        assert!(result.byte_estimation_accuracy >= 3);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_28() -> Result<()> {
        let result = tpcds_stats_vs_metrics(28).await?;
        assert!(result.row_estimation_accuracy >= 72);
        assert!(result.byte_estimation_accuracy >= 39);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_29() -> Result<()> {
        let result = tpcds_stats_vs_metrics(29).await?;
        assert!(result.row_estimation_accuracy >= 25);
        assert!(result.byte_estimation_accuracy >= 8);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_30() -> Result<()> {
        let result = tpcds_stats_vs_metrics(30).await?;
        assert!(result.row_estimation_accuracy >= 26);
        assert!(result.byte_estimation_accuracy >= 3);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_31() -> Result<()> {
        let result = tpcds_stats_vs_metrics(31).await?;
        assert!(result.row_estimation_accuracy >= 18);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_32() -> Result<()> {
        let result = tpcds_stats_vs_metrics(32).await?;
        assert!(result.row_estimation_accuracy >= 27);
        assert!(result.byte_estimation_accuracy >= 13);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_33() -> Result<()> {
        let result = tpcds_stats_vs_metrics(33).await?;
        assert!(result.row_estimation_accuracy >= 41);
        assert!(result.byte_estimation_accuracy >= 12);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_34() -> Result<()> {
        let result = tpcds_stats_vs_metrics(34).await?;
        assert!(result.row_estimation_accuracy >= 17);
        assert!(result.byte_estimation_accuracy >= 3);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_35() -> Result<()> {
        let result = tpcds_stats_vs_metrics(35).await?;
        assert!(result.row_estimation_accuracy >= 33);
        assert!(result.byte_estimation_accuracy >= 8);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_36() -> Result<()> {
        let result = tpcds_stats_vs_metrics(36).await?;
        assert!(result.row_estimation_accuracy >= 10);
        assert!(result.byte_estimation_accuracy >= 3);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_37() -> Result<()> {
        let result = tpcds_stats_vs_metrics(37).await?;
        assert!(result.row_estimation_accuracy >= 27);
        assert!(result.byte_estimation_accuracy >= 9);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_38() -> Result<()> {
        let result = tpcds_stats_vs_metrics(38).await?;
        assert!(result.row_estimation_accuracy >= 27);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_39() -> Result<()> {
        let result = tpcds_stats_vs_metrics(39).await?;
        assert!(result.row_estimation_accuracy >= 15);
        assert!(result.byte_estimation_accuracy >= 9);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_40() -> Result<()> {
        let result = tpcds_stats_vs_metrics(40).await?;
        assert!(result.row_estimation_accuracy >= 35);
        assert!(result.byte_estimation_accuracy >= 9);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_41() -> Result<()> {
        let result = tpcds_stats_vs_metrics(41).await?;
        assert!(result.row_estimation_accuracy >= 33);
        assert!(result.byte_estimation_accuracy >= 0);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_42() -> Result<()> {
        let result = tpcds_stats_vs_metrics(42).await?;
        assert!(result.row_estimation_accuracy >= 26);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_43() -> Result<()> {
        let result = tpcds_stats_vs_metrics(43).await?;
        assert!(result.row_estimation_accuracy >= 23);
        assert!(result.byte_estimation_accuracy >= 3);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_44() -> Result<()> {
        let result = tpcds_stats_vs_metrics(44).await?;
        assert!(result.row_estimation_accuracy >= 20);
        assert!(result.byte_estimation_accuracy >= 3);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_45() -> Result<()> {
        let result = tpcds_stats_vs_metrics(45).await?;
        assert!(result.row_estimation_accuracy >= 20);
        assert!(result.byte_estimation_accuracy >= 9);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_46() -> Result<()> {
        let result = tpcds_stats_vs_metrics(46).await?;
        assert!(result.row_estimation_accuracy >= 27);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_47() -> Result<()> {
        let result = tpcds_stats_vs_metrics(47).await?;
        assert!(result.row_estimation_accuracy >= 20);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_48() -> Result<()> {
        let result = tpcds_stats_vs_metrics(48).await?;
        assert!(result.row_estimation_accuracy >= 58);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_49() -> Result<()> {
        let result = tpcds_stats_vs_metrics(49).await?;
        assert!(result.row_estimation_accuracy >= 19);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_50() -> Result<()> {
        let result = tpcds_stats_vs_metrics(50).await?;
        assert!(result.row_estimation_accuracy >= 14);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_51() -> Result<()> {
        let result = tpcds_stats_vs_metrics(51).await?;
        assert!(result.row_estimation_accuracy >= 53);
        assert!(result.byte_estimation_accuracy >= 15);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_52() -> Result<()> {
        let result = tpcds_stats_vs_metrics(52).await?;
        assert!(result.row_estimation_accuracy >= 25);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_53() -> Result<()> {
        let result = tpcds_stats_vs_metrics(53).await?;
        assert!(result.row_estimation_accuracy >= 17);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_54() -> Result<()> {
        let result = tpcds_stats_vs_metrics(54).await?;
        assert!(result.row_estimation_accuracy >= 35);
        assert!(result.byte_estimation_accuracy >= 10);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_55() -> Result<()> {
        let result = tpcds_stats_vs_metrics(55).await?;
        assert!(result.row_estimation_accuracy >= 36);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_56() -> Result<()> {
        let result = tpcds_stats_vs_metrics(56).await?;
        assert!(result.row_estimation_accuracy >= 25);
        assert!(result.byte_estimation_accuracy >= 9);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_57() -> Result<()> {
        let result = tpcds_stats_vs_metrics(57).await?;
        assert!(result.row_estimation_accuracy >= 17);
        assert!(result.byte_estimation_accuracy >= 5);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_58() -> Result<()> {
        let result = tpcds_stats_vs_metrics(58).await?;
        assert!(result.row_estimation_accuracy >= 16);
        assert!(result.byte_estimation_accuracy >= 11);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_59() -> Result<()> {
        let result = tpcds_stats_vs_metrics(59).await?;
        assert!(result.row_estimation_accuracy >= 29);
        assert!(result.byte_estimation_accuracy >= 4);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_60() -> Result<()> {
        let result = tpcds_stats_vs_metrics(60).await?;
        assert!(result.row_estimation_accuracy >= 30);
        assert!(result.byte_estimation_accuracy >= 9);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_61() -> Result<()> {
        let result = tpcds_stats_vs_metrics(61).await?;
        assert!(result.row_estimation_accuracy >= 46);
        assert!(result.byte_estimation_accuracy >= 13);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_62() -> Result<()> {
        let result = tpcds_stats_vs_metrics(62).await?;
        assert!(result.row_estimation_accuracy >= 52);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_63() -> Result<()> {
        let result = tpcds_stats_vs_metrics(63).await?;
        assert!(result.row_estimation_accuracy >= 17);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_64() -> Result<()> {
        let result = tpcds_stats_vs_metrics(64).await?;
        assert!(result.row_estimation_accuracy >= 35);
        assert!(result.byte_estimation_accuracy >= 19);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_65() -> Result<()> {
        let result = tpcds_stats_vs_metrics(65).await?;
        assert!(result.row_estimation_accuracy >= 56);
        assert!(result.byte_estimation_accuracy >= 8);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_66() -> Result<()> {
        let result = tpcds_stats_vs_metrics(66).await?;
        assert!(result.row_estimation_accuracy >= 27);
        assert!(result.byte_estimation_accuracy >= 4);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_67() -> Result<()> {
        let result = tpcds_stats_vs_metrics(67).await?;
        assert!(result.row_estimation_accuracy >= 27);
        assert!(result.byte_estimation_accuracy >= 3);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_68() -> Result<()> {
        let result = tpcds_stats_vs_metrics(68).await?;
        assert!(result.row_estimation_accuracy >= 21);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_69() -> Result<()> {
        let result = tpcds_stats_vs_metrics(69).await?;
        assert!(result.row_estimation_accuracy >= 28);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_70() -> Result<()> {
        let result = tpcds_stats_vs_metrics(70).await?;
        assert!(result.row_estimation_accuracy >= 42);
        assert!(result.byte_estimation_accuracy >= 4);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_71() -> Result<()> {
        let result = tpcds_stats_vs_metrics(71).await?;
        assert!(result.row_estimation_accuracy >= 16);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_72() -> Result<()> {
        let result = tpcds_stats_vs_metrics(72).await?;
        assert!(result.row_estimation_accuracy >= 40);
        assert!(result.byte_estimation_accuracy >= 11);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_73() -> Result<()> {
        let result = tpcds_stats_vs_metrics(73).await?;
        assert!(result.row_estimation_accuracy >= 17);
        assert!(result.byte_estimation_accuracy >= 3);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_74() -> Result<()> {
        let result = tpcds_stats_vs_metrics(74).await?;
        assert!(result.row_estimation_accuracy >= 23);
        assert!(result.byte_estimation_accuracy >= 5);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_75() -> Result<()> {
        let result = tpcds_stats_vs_metrics(75).await?;
        assert!(result.row_estimation_accuracy >= 28);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_76() -> Result<()> {
        let result = tpcds_stats_vs_metrics(76).await?;
        assert!(result.row_estimation_accuracy >= 23);
        assert!(result.byte_estimation_accuracy >= 10);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_77() -> Result<()> {
        let result = tpcds_stats_vs_metrics(77).await?;
        assert!(result.row_estimation_accuracy >= 9);
        assert!(result.byte_estimation_accuracy >= 11);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_78() -> Result<()> {
        let result = tpcds_stats_vs_metrics(78).await?;
        assert!(result.row_estimation_accuracy >= 38);
        assert!(result.byte_estimation_accuracy >= 22);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_79() -> Result<()> {
        let result = tpcds_stats_vs_metrics(79).await?;
        assert!(result.row_estimation_accuracy >= 24);
        assert!(result.byte_estimation_accuracy >= 4);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_80() -> Result<()> {
        let result = tpcds_stats_vs_metrics(80).await?;
        assert!(result.row_estimation_accuracy >= 30);
        assert!(result.byte_estimation_accuracy >= 10);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_81() -> Result<()> {
        let result = tpcds_stats_vs_metrics(81).await?;
        assert!(result.row_estimation_accuracy >= 21);
        assert!(result.byte_estimation_accuracy >= 3);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_82() -> Result<()> {
        let result = tpcds_stats_vs_metrics(82).await?;
        assert!(result.row_estimation_accuracy >= 27);
        assert!(result.byte_estimation_accuracy >= 9);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_83() -> Result<()> {
        let result = tpcds_stats_vs_metrics(83).await?;
        assert!(result.row_estimation_accuracy >= 16);
        assert!(result.byte_estimation_accuracy >= 10);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_84() -> Result<()> {
        let result = tpcds_stats_vs_metrics(84).await?;
        assert!(result.row_estimation_accuracy >= 36);
        assert!(result.byte_estimation_accuracy >= 14);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_85() -> Result<()> {
        let result = tpcds_stats_vs_metrics(85).await?;
        assert!(result.row_estimation_accuracy >= 41);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_86() -> Result<()> {
        let result = tpcds_stats_vs_metrics(86).await?;
        assert!(result.row_estimation_accuracy >= 19);
        assert!(result.byte_estimation_accuracy >= 6);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_87() -> Result<()> {
        let result = tpcds_stats_vs_metrics(87).await?;
        assert!(result.row_estimation_accuracy >= 26);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_88() -> Result<()> {
        let result = tpcds_stats_vs_metrics(88).await?;
        assert!(result.row_estimation_accuracy >= 57);
        assert!(result.byte_estimation_accuracy >= 19);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_89() -> Result<()> {
        let result = tpcds_stats_vs_metrics(89).await?;
        assert!(result.row_estimation_accuracy >= 22);
        assert!(result.byte_estimation_accuracy >= 3);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Error: ArrowError(DivideByZero, Some(\"\"))"]
    async fn tpcds_90() -> Result<()> {
        let _result = tpcds_stats_vs_metrics(90).await?;
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_91() -> Result<()> {
        let result = tpcds_stats_vs_metrics(91).await?;
        assert!(result.row_estimation_accuracy >= 38);
        assert!(result.byte_estimation_accuracy >= 7);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_92() -> Result<()> {
        let result = tpcds_stats_vs_metrics(92).await?;
        assert!(result.row_estimation_accuracy >= 29);
        assert!(result.byte_estimation_accuracy >= 15);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_93() -> Result<()> {
        let result = tpcds_stats_vs_metrics(93).await?;
        assert!(result.row_estimation_accuracy >= 41);
        assert!(result.byte_estimation_accuracy >= 9);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_94() -> Result<()> {
        let result = tpcds_stats_vs_metrics(94).await?;
        assert!(result.row_estimation_accuracy >= 43);
        assert!(result.byte_estimation_accuracy >= 12);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_95() -> Result<()> {
        let result = tpcds_stats_vs_metrics(95).await?;
        assert!(result.row_estimation_accuracy >= 42);
        assert!(result.byte_estimation_accuracy >= 17);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_96() -> Result<()> {
        let result = tpcds_stats_vs_metrics(96).await?;
        assert!(result.row_estimation_accuracy >= 55);
        assert!(result.byte_estimation_accuracy >= 15);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_97() -> Result<()> {
        let result = tpcds_stats_vs_metrics(97).await?;
        assert!(result.row_estimation_accuracy >= 54);
        assert!(result.byte_estimation_accuracy >= 10);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_98() -> Result<()> {
        let result = tpcds_stats_vs_metrics(98).await?;
        assert!(result.row_estimation_accuracy >= 22);
        assert!(result.byte_estimation_accuracy >= 4);
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_99() -> Result<()> {
        let result = tpcds_stats_vs_metrics(99).await?;
        assert!(result.row_estimation_accuracy >= 44);
        assert!(result.byte_estimation_accuracy >= 4);
        Ok(())
    }

    struct AccuracyResult {
        row_estimation_accuracy: usize,
        byte_estimation_accuracy: usize,
    }

    async fn tpcds_stats_vs_metrics(query_no: usize) -> Result<AccuracyResult> {
        let filename = format!("tests/tpc-ds/{query_no}.sql");
        let sql = fs::read_to_string(filename)?;

        let ctx = small_tpcds_ctx().await;
        let mut df = None;
        for query in sql.split(';').filter(|s| !s.trim().is_empty()) {
            df = Some(ctx.sql(query).await?);
        }
        let df = df.unwrap();
        let plan = df.create_physical_plan().await?;
        collect(plan.clone(), ctx.task_ctx()).await?;
        let node = Node::from_plan(
            &plan,
            StatsVsMetricsDisplayOptions {
                display_output_bytes: true,
                display_output_rows: true,
            },
        )?;
        println!("{node:?}");
        Ok(AccuracyResult {
            row_estimation_accuracy: node.avg_row_accuracy(),
            byte_estimation_accuracy: node.avg_byte_accuracy(),
        })
    }

    async fn small_tpcds_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        for entry in fs::read_dir("tests/data").expect("could not read tests/data dir") {
            let path = entry
                .expect("could not get entry from tests/data dir")
                .path();
            let file_name = path.file_name().unwrap().to_str().unwrap();
            if file_name.starts_with("tpcds") {
                let table_name = file_name
                    .trim_start_matches("tpcds_")
                    .trim_end_matches("_small.parquet");
                ctx.register_parquet(
                    table_name,
                    path.to_str().unwrap(),
                    ParquetReadOptions::default(),
                )
                .await
                .expect("Could not register parquet file as table");
            }
        }
        ctx
    }
}
