package com.contactsunny.poc.sparkSqlUdfPoc.domain;

import com.contactsunny.poc.sparkSqlUdfPoc.enums.Level;

public class TempLingValue {
  private Level level;
  private int lower;
  private int lowerMid;
  private int upperMid;
  private int upper;

  public TempLingValue(Level level, int lower, int lowerMid, int upperMid, int upper) {
    this.level = level;
    this.lower = lower;
    this.lowerMid = lowerMid;
    this.upperMid = upperMid;
    this.upper = upper;
  }

  public Level getLevel() {
    return level;
  }

  public int getLower() {
    return lower;
  }

  public int getLowerMid() {
    return lowerMid;
  }

  public int getUpperMid() {
    return upperMid;
  }

  public int getUpper() {
    return upper;
  }
}
