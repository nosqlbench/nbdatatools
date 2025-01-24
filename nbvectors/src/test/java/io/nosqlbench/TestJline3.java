package io.nosqlbench;


import org.jline.curses.Curses;
import org.jline.curses.GUI;
import org.jline.curses.Window;
import org.jline.curses.impl.BasicWindow;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestJline3 {
  @Test
  @Disabled
  public void test() {
    try {
      Terminal terminal =
          TerminalBuilder.terminal();
      GUI gui = Curses.gui(terminal);
      Window window = new BasicWindow();
      window.setTitle("TestJLine3");
      gui.addWindow(window);
      Thread.sleep(5000);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
