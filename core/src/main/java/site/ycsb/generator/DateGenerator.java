package site.ycsb.generator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import static site.ycsb.Utils.addPadding;

/**
 * Created by rgmacedo on 3/7/17.
 */
public class DateGenerator extends Generator<String> {
  private int startYear;
  private int endYear;

  public DateGenerator(int startYear, int endYear) {
    this.startYear = startYear;
    this.endYear = endYear;
    System.out.println("Date Generator initialized");
  }

  @Override
  public String nextValue() {
    GregorianCalendar gc = new GregorianCalendar();
    int year = startYear + (int)(Math.random()*((endYear-startYear)+1));
    gc.set(Calendar.YEAR, year);

    int dayOfYear = 1 + (int)(Math.random()*((gc.getActualMaximum(Calendar.DAY_OF_YEAR)-1)+1));
    gc.set(Calendar.DAY_OF_YEAR, dayOfYear);

    return gc.get(Calendar.YEAR)
        +"/"+addPadding(String.valueOf(gc.get(Calendar.MONTH)+1), 2, '0')
        +"/"+addPadding(String.valueOf(gc.get(Calendar.DAY_OF_MONTH)), 2, '0');
  }

  public long dateToTimestamp(String date) {
    long timestamp = 0;
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
      Date tempDate = sdf.parse(date);
      Calendar tempCalendar = Calendar.getInstance();
      tempCalendar.setTime(tempDate);
      timestamp = tempCalendar.getTimeInMillis();
//      System.out.println("Timestamp: "+date+" - "+timestamp);
    } catch (ParseException e) {
      System.out.println("Date To Timestamp - ParseException: "+e.getMessage());
    }
    return timestamp;
  }

  @Override
  public String lastValue() {
    return "not a date";
  }
}

