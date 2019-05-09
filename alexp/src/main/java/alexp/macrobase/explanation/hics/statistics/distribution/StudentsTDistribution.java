package alexp.macrobase.explanation.hics.statistics.distribution;

/*
 This file is part of ELKI:
 Environment for Developing KDD-Applications Supported by Index-Structures

 Copyright (C) 2012
 Ludwig-Maximilians-Universität München
 Lehr- und Forschungseinheit für Datenbanksysteme
 ELKI Development Team

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


import alexp.macrobase.explanation.utils.RandomFactory;

import java.util.Random;

/**
 * Student's t distribution.
 * 
 * FIXME: add quantile and random function!
 * 
 * @author Jan Brusis
 */
public class StudentsTDistribution extends AbstractDistribution {
  /**
   * Degrees of freedom
   */
  private final int v;

  /**
   * Constructor.
   * 
   * @param v Degrees of freedom
   */
  public StudentsTDistribution(int v) {
    this(v, (Random) null);
  }

  /**
   * Constructor.
   * 
   * @param v Degrees of freedom
   * @param random Random generator
   */
  public StudentsTDistribution(int v, Random random) {
    super(random);
    this.v = v;
  }

  /**
   * Constructor.
   * 
   * @param v Degrees of freedom
   * @param random Random generator
   */
  public StudentsTDistribution(int v, RandomFactory random) {
    super(random);
    this.v = v;
  }

  public double pdf(double val) {
    return pdf(val, v);
  }

  public double cdf(double val) {
    return cdf(val, v);
  }

  public double quantile(double val) {
    return 0;
  }

  /**
   * Static version of the t distribution's PDF.
   * 
   * @param val value to evaluate
   * @param v degrees of freedom
   * @return f(val,v)
   */
  public static double pdf(double val, int v) {
    // TODO: improve precision by computing "exp" last?
    return Math.exp(GammaDistribution.logGamma((v + 1) * .5) - GammaDistribution.logGamma(v * .5)) * (1 / Math.sqrt(v * Math.PI)) * Math.pow(1 + (val * val) / v, -((v + 1) * .5));
  }

  /**
   * Static version of the CDF of the t-distribution for t > 0
   *
   * @param val value to evaluate
   * @param v degrees of freedom
   * @return F(val, v)
   */
  public static double cdf(double val, int v) {
    double x = v / (val * val + v);
    return 1 - (0.5 * BetaDistribution.regularizedIncBeta(x, v * .5, 0.5));
  }



  @Override
  public String toString() {
    return "StudentsTDistribution(v=" + v + ")";
  }

}
