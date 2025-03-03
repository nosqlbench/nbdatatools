package io.nosqlbench.nbvectors.verifyknn.statusview;

/*
 * Copyright (c) nosqlbench
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.graphics.*;
import com.googlecode.lanterna.gui2.*;
import com.googlecode.lanterna.input.KeyStroke;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.Terminal;
import com.googlecode.lanterna.terminal.ansi.UnixLikeTerminal;
import com.googlecode.lanterna.terminal.ansi.UnixTerminal;
import com.googlecode.lanterna.terminal.virtual.DefaultVirtualTerminal;
import io.nosqlbench.nbvectors.verifyknn.datatypes.LongIndexedFloatVector;
import io.nosqlbench.nbvectors.verifyknn.computation.NeighborhoodComparison;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class StatusViewLanterna implements AutoCloseable, StatusView {

  private final Terminal terminal;
  private final TerminalScreen screen;
  private final MultiWindowTextGUI gui;
  private final int summaries;
  private BasicWindow statusWindow;
  private Panel content;
  private ProgressBar intervalProgress;
  private ProgressBar chunkProgress;
  private TextBox[] lastComparisonText;
  private BasicTextImage textimage;
  private LongIndexedFloatVector lastQueryVector;
  private int totalQueryVectors;
  private int currentQueryVector;

  public StatusViewLanterna(int summaries) {
    this.summaries = summaries;
    this.lastComparisonText = new TextBox[summaries];

    Terminal terminal = null;
    try {
      terminal = new UnixTerminal(
          System.in,
          System.out,
          StandardCharsets.UTF_8,
          UnixLikeTerminal.CtrlCBehaviour.CTRL_C_KILLS_APPLICATION
      );
    } catch (IOException e) {
      System.err.println("Unable to create terminal, using virtual terminal instead");
      terminal = new DefaultVirtualTerminal();
    }
    this.terminal = terminal;

    try {
      this.screen = new TerminalScreen(terminal);
      screen.startScreen();
      gui = new MultiWindowTextGUI(screen, new DefaultWindowManager(), null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    initLayout();
  }


  @Override
  public void close() throws Exception {
    screen.stopScreen(true);
    screen.close();
  }

  private void initLayout() {
    statusWindow = new BasicWindow("Status");
    statusWindow.setHints(List.of(Window.Hint.FIT_TERMINAL_WINDOW, Window.Hint.NO_DECORATIONS));
    //    statusWindow.setHints(List.of(Window.Hint.FULL_SCREEN,Window.Hint.EXPANDED));
    //        Window.Hint.FIT_TERMINAL_WINDOW
    //        ,
    //        Window.Hint.NO_DECORATIONS
    gui.addWindow(statusWindow);
    //    gui.setActiveWindow(statusWindow);

    content = new Panel(new LinearLayout(Direction.VERTICAL));
    SimpleTheme customtheme = SimpleTheme.makeTheme(
        true,
        new TextColor.RGB(51, 200, 0),
        new TextColor.RGB(40, 40, 40),
        new TextColor.RGB(200, 180, 220),
        new TextColor.RGB(80, 80, 80),
        new TextColor.RGB(200, 200, 200),
        new TextColor.RGB(100, 100, 100),
        new TextColor.RGB(40, 40, 40)
    );
    gui.setTheme(customtheme);

    content.addComponent(new Label("NBVector: KNN Answer Key Verifier"));
    statusWindow.setComponent(content);
    intervalProgress = new ProgressBar(0, 100, screen.getTerminalSize().getColumns());
    content.addComponent(intervalProgress);
    chunkProgress = new ProgressBar(0, 100, screen.getTerminalSize().getColumns());
    content.addComponent(chunkProgress);

    for (int i = 0; i < summaries; i++) {
      lastComparisonText[i] =
          new TextBox(new TerminalSize(screen.getTerminalSize().getColumns(), 3));
      content.addComponent(lastComparisonText[i]);
    }
    try {
      gui.updateScreen();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            KeyStroke keyStroke = screen.readInput();
            System.out.println(keyStroke);
            screen.refresh();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }).start();
  }


  int update = 0;

  @Override
  public void onStart(int totalQueryVectors) {
    this.totalQueryVectors = totalQueryVectors;
  }

  @Override
  public void onChunk(int chunk, int chunkSize, int totalTrainingVectors) {
    chunkProgress.setLabelFormat("chunk " + chunk + "/" + totalTrainingVectors + " (%2.0f%%)");
    chunkProgress.setMax(totalTrainingVectors);
    chunkProgress.setValue(chunk);
    try {
      gui.updateScreen();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onQueryVector(LongIndexedFloatVector vector, long index, long end) {
    currentQueryVector++;
    lastQueryVector = vector;
    intervalProgress.setMax(totalQueryVectors);
    intervalProgress.setValue(currentQueryVector);
    intervalProgress.setLabelFormat(
        "query " + currentQueryVector + "/" + totalQueryVectors + " index[" + index + "]"
        + " (%2.0f%%)");
    int modulo = (int) (vector.index() % summaries);
    lastComparisonText[modulo].setText(lastQueryVector.toString());
    try {
      gui.updateScreen();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onNeighborhoodComparison(NeighborhoodComparison comparison) {
    int modulo = (int) (comparison.testVector().index() % summaries);
    lastComparisonText[modulo].setText(lastQueryVector + "\n" + comparison.toString());
    try {
      gui.updateScreen();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void end() {
  }
}
