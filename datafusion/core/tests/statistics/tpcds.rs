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

use crate::statistics::StatsVsMetricsDisplayOptions;
use datafusion::common::Result;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_physical_plan::collect;
use std::fs;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statistics::Node;
    use insta::assert_snapshot;
    use std::fmt::{Display, Formatter};

    #[tokio::test]
    async fn tpcds_1() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(1).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=20%
        byte_estimation_accuracy=8%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_2() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(2).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=34%
        byte_estimation_accuracy=17%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_3() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(3).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=44%
        byte_estimation_accuracy=10%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_4() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(4).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=22%
        byte_estimation_accuracy=4%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_5() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(5).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=21%
        byte_estimation_accuracy=12%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_6() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(6).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=28%
        byte_estimation_accuracy=5%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_7() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(7).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=34%
        byte_estimation_accuracy=4%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_8() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(8).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=34%
        byte_estimation_accuracy=2%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_9() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(9).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=58%
        byte_estimation_accuracy=32%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_10() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(10).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=33%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_11() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(11).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=23%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_12() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(12).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=22%
        byte_estimation_accuracy=5%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_13() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(13).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=58%
        byte_estimation_accuracy=9%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_14() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(14).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=37%
        byte_estimation_accuracy=16%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_15() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(15).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=15%
        byte_estimation_accuracy=8%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_16() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(16).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=24%
        byte_estimation_accuracy=9%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_17() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(17).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=11%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_18() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(18).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=36%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_19() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(19).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=36%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_20() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(20).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=22%
        byte_estimation_accuracy=5%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_21() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(21).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=31%
        byte_estimation_accuracy=3%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_22() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(22).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=18%
        byte_estimation_accuracy=2%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_23() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(23).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=19%
        byte_estimation_accuracy=12%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_24() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(24).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=33%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_25() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(25).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=18%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_26() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(26).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=31%
        byte_estimation_accuracy=2%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_27() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(27).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=20%
        byte_estimation_accuracy=3%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_28() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(28).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=73%
        byte_estimation_accuracy=40%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_29() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(29).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=25%
        byte_estimation_accuracy=8%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_30() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(30).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=26%
        byte_estimation_accuracy=3%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_31() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(31).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=18%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_32() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(32).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=27%
        byte_estimation_accuracy=13%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_33() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(33).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=41%
        byte_estimation_accuracy=12%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_34() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(34).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=17%
        byte_estimation_accuracy=3%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_35() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(35).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=33%
        byte_estimation_accuracy=8%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_36() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(36).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=10%
        byte_estimation_accuracy=3%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_37() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(37).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=27%
        byte_estimation_accuracy=9%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_38() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(38).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=27%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_39() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(39).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=15%
        byte_estimation_accuracy=9%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_40() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(40).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=35%
        byte_estimation_accuracy=9%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_41() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(41).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=33%
        byte_estimation_accuracy=0%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_42() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(42).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=26%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_43() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(43).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=23%
        byte_estimation_accuracy=3%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_44() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(44).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=20%
        byte_estimation_accuracy=3%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_45() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(45).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=20%
        byte_estimation_accuracy=9%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_46() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(46).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=27%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_47() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(47).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=20%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_48() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(48).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=64%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_49() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(49).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=19%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_50() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(50).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=14%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_51() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(51).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=53%
        byte_estimation_accuracy=17%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_52() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(52).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=25%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_53() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(53).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=17%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_54() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(54).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=35%
        byte_estimation_accuracy=10%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_55() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(55).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=36%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_56() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(56).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=25%
        byte_estimation_accuracy=9%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_57() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(57).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=17%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_58() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(58).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=16%
        byte_estimation_accuracy=11%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_59() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(59).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=29%
        byte_estimation_accuracy=5%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_60() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(60).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=30%
        byte_estimation_accuracy=9%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_61() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(61).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=46%
        byte_estimation_accuracy=13%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_62() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(62).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=52%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_63() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(63).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=17%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_64() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(64).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=35%
        byte_estimation_accuracy=19%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_65() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(65).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=57%
        byte_estimation_accuracy=8%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_66() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(66).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=28%
        byte_estimation_accuracy=4%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_67() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(67).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=27%
        byte_estimation_accuracy=3%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_68() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(68).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=21%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_69() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(69).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=28%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_70() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(70).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=42%
        byte_estimation_accuracy=4%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_71() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(71).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=16%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_72() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(72).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=40%
        byte_estimation_accuracy=11%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_73() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(73).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=17%
        byte_estimation_accuracy=3%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_74() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(74).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=23%
        byte_estimation_accuracy=5%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_75() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(75).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=28%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_76() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(76).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=23%
        byte_estimation_accuracy=10%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_77() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(77).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=9%
        byte_estimation_accuracy=11%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_78() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(78).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=38%
        byte_estimation_accuracy=22%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_79() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(79).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=24%
        byte_estimation_accuracy=4%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_80() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(80).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=30%
        byte_estimation_accuracy=10%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_81() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(81).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=21%
        byte_estimation_accuracy=3%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_82() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(82).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=27%
        byte_estimation_accuracy=9%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_83() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(83).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=16%
        byte_estimation_accuracy=10%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_84() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(84).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=36%
        byte_estimation_accuracy=14%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_85() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(85).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=41%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_86() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(86).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=19%
        byte_estimation_accuracy=6%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_87() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(87).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=26%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_88() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(88).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=60%
        byte_estimation_accuracy=19%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_89() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(89).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=22%
        byte_estimation_accuracy=3%
        ");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Error: ArrowError(DivideByZero, Some(\"\"))"]
    async fn tpcds_90() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(90).await?;
        assert_snapshot!(display, @"");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_91() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(91).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=38%
        byte_estimation_accuracy=7%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_92() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(92).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=29%
        byte_estimation_accuracy=15%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_93() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(93).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=41%
        byte_estimation_accuracy=9%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_94() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(94).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=43%
        byte_estimation_accuracy=12%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_95() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(95).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=42%
        byte_estimation_accuracy=17%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_96() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(96).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=59%
        byte_estimation_accuracy=15%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_97() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(97).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=55%
        byte_estimation_accuracy=10%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_98() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(98).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=22%
        byte_estimation_accuracy=4%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn tpcds_99() -> Result<()> {
        let display = tpcds_stats_vs_metrics_display(99).await?;
        assert_snapshot!(display, @r"
        row_estimation_accuracy=44%
        byte_estimation_accuracy=4%
        ");
        Ok(())
    }

    struct AccuracyResult {
        row_estimation_accuracy: usize,
        byte_estimation_accuracy: usize,
    }

    impl Display for AccuracyResult {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            writeln!(
                f,
                "row_estimation_accuracy={}%",
                self.row_estimation_accuracy
            )?;
            writeln!(
                f,
                "byte_estimation_accuracy={}%",
                self.byte_estimation_accuracy
            )
        }
    }

    async fn tpcds_stats_vs_metrics_display(query_no: usize) -> Result<AccuracyResult> {
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
