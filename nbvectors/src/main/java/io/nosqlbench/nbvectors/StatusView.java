package io.nosqlbench.nbvectors;

import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.googlecode.lanterna.terminal.Terminal;
import com.googlecode.lanterna.terminal.virtual.DefaultVirtualTerminal;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StatusView implements AutoCloseable {

  private final Terminal terminal;
  private final TerminalScreen screen;

  public StatusView() {
    Terminal terminal = null;
    try {
      terminal = new DefaultTerminalFactory(
          System.out,
          System.in,
          StandardCharsets.UTF_8
      ).createTerminal();
      terminal.enterPrivateMode();
    } catch (IOException e) {
      terminal = new DefaultVirtualTerminal();
    }
    this.terminal = terminal;

    try {
      this.screen = new TerminalScreen(terminal);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }


  public void onQueryVector(IndexedFloatVector vector) {

  }
  public void onNeighborhoodComparison(NeighborhoodComparison comparison) {
  }

  @Override
  public void close() throws Exception {

    screen.stopScreen(true);
    screen.close();
    terminal.exitPrivateMode();
  }
}
