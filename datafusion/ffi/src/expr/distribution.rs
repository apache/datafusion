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

use abi_stable::StableAbi;
use datafusion_common::DataFusionError;
use datafusion_expr::statistics::{
    BernoulliDistribution, Distribution, ExponentialDistribution, GaussianDistribution,
    GenericDistribution, UniformDistribution,
};

use crate::arrow_wrappers::WrappedArray;
use crate::expr::interval::FFI_Interval;

/// A stable struct for sharing [`Distribution`] across FFI boundaries.
/// See ['Distribution'] for the meaning of each variant.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[expect(clippy::large_enum_variant)]
pub enum FFI_Distribution {
    Uniform(FFI_UniformDistribution),
    Exponential(FFI_ExponentialDistribution),
    Gaussian(FFI_GaussianDistribution),
    Bernoulli(FFI_BernoulliDistribution),
    Generic(FFI_GenericDistribution),
}

impl TryFrom<&Distribution> for FFI_Distribution {
    type Error = DataFusionError;
    fn try_from(value: &Distribution) -> Result<Self, Self::Error> {
        match value {
            Distribution::Uniform(d) => Ok(FFI_Distribution::Uniform(d.try_into()?)),
            Distribution::Exponential(d) => {
                Ok(FFI_Distribution::Exponential(d.try_into()?))
            }
            Distribution::Gaussian(d) => Ok(FFI_Distribution::Gaussian(d.try_into()?)),
            Distribution::Bernoulli(d) => Ok(FFI_Distribution::Bernoulli(d.try_into()?)),
            Distribution::Generic(d) => Ok(FFI_Distribution::Generic(d.try_into()?)),
        }
    }
}

impl TryFrom<FFI_Distribution> for Distribution {
    type Error = DataFusionError;
    fn try_from(value: FFI_Distribution) -> Result<Self, Self::Error> {
        match value {
            FFI_Distribution::Uniform(d) => d.try_into(),
            FFI_Distribution::Exponential(d) => d.try_into(),
            FFI_Distribution::Gaussian(d) => d.try_into(),
            FFI_Distribution::Bernoulli(d) => d.try_into(),
            FFI_Distribution::Generic(d) => d.try_into(),
        }
    }
}

#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_UniformDistribution {
    interval: FFI_Interval,
}

#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_ExponentialDistribution {
    rate: WrappedArray,
    offset: WrappedArray,
    positive_tail: bool,
}

#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_GaussianDistribution {
    mean: WrappedArray,
    variance: WrappedArray,
}

#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_BernoulliDistribution {
    p: WrappedArray,
}

#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_GenericDistribution {
    mean: WrappedArray,
    median: WrappedArray,
    variance: WrappedArray,
    range: FFI_Interval,
}

impl TryFrom<&UniformDistribution> for FFI_UniformDistribution {
    type Error = DataFusionError;
    fn try_from(value: &UniformDistribution) -> Result<Self, Self::Error> {
        Ok(Self {
            interval: value.range().try_into()?,
        })
    }
}

impl TryFrom<&ExponentialDistribution> for FFI_ExponentialDistribution {
    type Error = DataFusionError;
    fn try_from(value: &ExponentialDistribution) -> Result<Self, Self::Error> {
        let rate = value.rate().try_into()?;
        let offset = value.offset().try_into()?;

        Ok(Self {
            rate,
            offset,
            positive_tail: value.positive_tail(),
        })
    }
}

impl TryFrom<&GaussianDistribution> for FFI_GaussianDistribution {
    type Error = DataFusionError;
    fn try_from(value: &GaussianDistribution) -> Result<Self, Self::Error> {
        let mean = value.mean().try_into()?;
        let variance = value.variance().try_into()?;

        Ok(Self { mean, variance })
    }
}

impl TryFrom<&BernoulliDistribution> for FFI_BernoulliDistribution {
    type Error = DataFusionError;
    fn try_from(value: &BernoulliDistribution) -> Result<Self, Self::Error> {
        let p = value.p_value().try_into()?;

        Ok(Self { p })
    }
}

impl TryFrom<&GenericDistribution> for FFI_GenericDistribution {
    type Error = DataFusionError;
    fn try_from(value: &GenericDistribution) -> Result<Self, Self::Error> {
        let mean = value.mean().try_into()?;
        let median = value.median().try_into()?;
        let variance = value.variance().try_into()?;

        Ok(Self {
            mean,
            median,
            variance,
            range: value.range().try_into()?,
        })
    }
}

impl TryFrom<FFI_UniformDistribution> for Distribution {
    type Error = DataFusionError;
    fn try_from(value: FFI_UniformDistribution) -> Result<Self, Self::Error> {
        let interval = value.interval.try_into()?;
        Distribution::new_uniform(interval)
    }
}

impl TryFrom<FFI_ExponentialDistribution> for Distribution {
    type Error = DataFusionError;
    fn try_from(value: FFI_ExponentialDistribution) -> Result<Self, Self::Error> {
        let rate = value.rate.try_into()?;
        let offset = value.offset.try_into()?;

        Distribution::new_exponential(rate, offset, value.positive_tail)
    }
}

impl TryFrom<FFI_GaussianDistribution> for Distribution {
    type Error = DataFusionError;
    fn try_from(value: FFI_GaussianDistribution) -> Result<Self, Self::Error> {
        let mean = value.mean.try_into()?;
        let variance = value.variance.try_into()?;

        Distribution::new_gaussian(mean, variance)
    }
}

impl TryFrom<FFI_BernoulliDistribution> for Distribution {
    type Error = DataFusionError;
    fn try_from(value: FFI_BernoulliDistribution) -> Result<Self, Self::Error> {
        let p = value.p.try_into()?;

        Distribution::new_bernoulli(p)
    }
}

impl TryFrom<FFI_GenericDistribution> for Distribution {
    type Error = DataFusionError;
    fn try_from(value: FFI_GenericDistribution) -> Result<Self, Self::Error> {
        let mean = value.mean.try_into()?;
        let median = value.median.try_into()?;
        let variance = value.variance.try_into()?;
        let range = value.range.try_into()?;

        Distribution::new_generic(mean, median, variance, range)
    }
}
