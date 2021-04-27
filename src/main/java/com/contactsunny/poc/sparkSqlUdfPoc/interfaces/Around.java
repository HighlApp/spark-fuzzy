package com.contactsunny.poc.sparkSqlUdfPoc.interfaces;

@FunctionalInterface
public interface Around {
  Double calculate(Integer columnVal, Integer lower, Integer lowerMid, Integer upperMid, Integer upper);
}
