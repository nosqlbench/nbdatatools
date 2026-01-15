package io.nosqlbench.vshapes.extract;

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

import io.nosqlbench.vshapes.model.*;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses ScalarModel specifications from string format.
 *
 * <p>Supports parsing the compact string format used in CLI output, including:
 * <ul>
 *   <li>Basic models: {@code 𝒩(0,1)[-1,1]}</li>
 *   <li>Composite models: {@code ⊕3[▭,𝒩,β]:[−0.5,0.5]*.33,𝒩(0,1)*.33,...}</li>
 * </ul>
 *
 * <p>Handles Unicode glyphs and common formatting variations (e.g., Unicode minus sign).
 */
public class ModelParser {

    // Glyphs matching CMD_analyze_profile
    private static final String GLYPH_NORMAL = "𝒩";       // U+1D4A9
    private static final String GLYPH_UNIFORM = "▭";      // U+25AD
    private static final String GLYPH_BETA = "β";         // U+03B2
    private static final String GLYPH_GAMMA = "Γ";        // U+0393
    private static final String GLYPH_INV_GAMMA = "Γ⁻¹";  // Gamma with superscript -1
    private static final String GLYPH_STUDENT_T = "𝑡";    // U+1D461
    private static final String GLYPH_PEARSON_IV = "ℙ";   // U+2119
    private static final String GLYPH_BETA_PRIME = "β′";  // Beta with prime
    private static final String GLYPH_COMPOSITE = "⊕";    // U+2295
    
    // Fallback for broken surrogate pairs or unknown glyphs
    private static final String BROKEN_GLYPH = "?";

    /**
     * Parses a model specification string into a ScalarModel.
     *
     * @param spec the model specification string
     * @return the parsed ScalarModel
     * @throws IllegalArgumentException if the format is invalid
     */
    public static ScalarModel parse(String spec) {
        if (spec == null || spec.isBlank()) {
            throw new IllegalArgumentException("Model specification cannot be empty");
        }
        
        spec = spec.trim();
        
        if (spec.startsWith(GLYPH_COMPOSITE)) {
            return parseComposite(spec);
        }
        
        return parseBasic(spec);
    }

    private static ScalarModel parseComposite(String spec) {
        // Format: ⊕3[▭,𝒩,β]:[−0.58,−0.43]*.33,?(0,0.07)[−1,1]*.35,...
        
        // 1. Parse header: ⊕3[types]
        int colonIdx = spec.indexOf(':');
        String header = (colonIdx >= 0) ? spec.substring(0, colonIdx) : spec;
        String body = (colonIdx >= 0) ? spec.substring(colonIdx + 1) : "";
        
        // Parse types list
        int bracketStart = header.indexOf('[');
        int bracketEnd = header.indexOf(']');
        
        if (bracketStart < 0 || bracketEnd < 0) {
            throw new IllegalArgumentException("Invalid composite format (missing types list): " + spec);
        }
        
        String typesStr = header.substring(bracketStart + 1, bracketEnd);
        String[] typeGlyphs = typesStr.split(",");
        
        // Parse components
        List<String> componentSpecs = splitComponents(body);
        
        if (typeGlyphs.length != componentSpecs.size()) {
            throw new IllegalArgumentException(String.format(
                "Mismatch between types count (%d) and components count (%d): %s",
                typeGlyphs.length, componentSpecs.size(), spec));
        }
        
        ScalarModel[] models = new ScalarModel[typeGlyphs.length];
        double[] weights = new double[typeGlyphs.length];
        
        for (int i = 0; i < typeGlyphs.length; i++) {
            String glyph = typeGlyphs[i].trim();
            String compSpec = componentSpecs.get(i).trim();
            
            // Parse weight (*w)
            int weightIdx = compSpec.lastIndexOf('*');
            if (weightIdx >= 0) {
                String weightStr = compSpec.substring(weightIdx + 1);
                // Handle .33 (without leading zero)
                if (weightStr.startsWith(".")) weightStr = "0" + weightStr;
                weights[i] = Double.parseDouble(weightStr);
                compSpec = compSpec.substring(0, weightIdx);
            } else {
                weights[i] = 1.0; // Default
            }
            
            // Reconstruct full basic spec for parsing: Glyph + Params
            // If compSpec already has the glyph (e.g., copy-pasted with params), use it.
            // Otherwise prepend the glyph from the type list.
            String fullSpec;
            if (startsWithGlyph(compSpec)) {
                fullSpec = compSpec;
            } else if (compSpec.startsWith(BROKEN_GLYPH)) {
                 // Replace broken glyph with correct one from type list
                 fullSpec = glyph + compSpec.substring(1);
            } else {
                fullSpec = glyph + compSpec;
            }
            
            models[i] = parseBasic(fullSpec);
        }
        
        // Normalize weights
        double sumWeights = 0;
        for (double w : weights) sumWeights += w;
        if (sumWeights > 0) {
            for (int i = 0; i < weights.length; i++) weights[i] /= sumWeights;
        }
        
        return new CompositeScalarModel(Arrays.asList(models), weights);
    }
    
    private static ScalarModel parseBasic(String spec) {
        // Identify type
        String typeGlyph = identifyGlyph(spec);
        String paramsPart = spec;
        
        if (typeGlyph != null) {
            // Strip glyph if present (might be multi-char)
            if (spec.startsWith(typeGlyph)) {
                paramsPart = spec.substring(typeGlyph.length());
            } else if (spec.startsWith(BROKEN_GLYPH)) {
                paramsPart = spec.substring(BROKEN_GLYPH.length());
            }
        } else {
            // No known glyph found, try to infer or fail?
            // User example: ?(0,0.07)...
            if (spec.startsWith(BROKEN_GLYPH)) {
                // Assume Normal if ambiguous? Or check params count?
                // For now, let's treat ? as Normal default if we can't tell
                // But better to rely on what parserComposite passes us (it passes the correct glyph)
                // If parseBasic is called directly with ?, it's ambiguous.
                // Assuming Normal is risky but ? usually meant Normal in previous context.
                typeGlyph = GLYPH_NORMAL;
                paramsPart = spec.substring(1);
            } else {
                 throw new IllegalArgumentException("Unknown model type glyph in: " + spec);
            }
        }
        
        // Parse parameters (a,b,c)
        List<Double> params = new ArrayList<>();
        int parenStart = paramsPart.indexOf('(');
        int parenEnd = paramsPart.indexOf(')');
        if (parenStart >= 0 && parenEnd > parenStart) {
            String pStr = paramsPart.substring(parenStart + 1, parenEnd);
            for (String p : pStr.split(",")) {
                params.add(parseValue(p));
            }
        }
        
        // Parse bounds [min,max]
        Double lower = null;
        Double upper = null;
        int bracketStart = paramsPart.indexOf('[');
        int bracketEnd = paramsPart.indexOf(']');
        if (bracketStart >= 0 && bracketEnd > bracketStart) {
            String bStr = paramsPart.substring(bracketStart + 1, bracketEnd);
            String[] bounds = bStr.split(",");
            if (bounds.length == 2) {
                lower = parseValue(bounds[0]);
                upper = parseValue(bounds[1]);
            }
        }
        
        return createModel(typeGlyph, params, lower, upper);
    }
    
    private static ScalarModel createModel(String glyph, List<Double> params, Double lower, Double upper) {
        if (GLYPH_NORMAL.equals(glyph)) {
            double mean = params.size() > 0 ? params.get(0) : 0.0;
            double stdDev = params.size() > 1 ? params.get(1) : 1.0;
            if (lower != null && upper != null) {
                return new NormalScalarModel(mean, stdDev, lower, upper);
            }
            return new NormalScalarModel(mean, stdDev);
        }
        
        if (GLYPH_UNIFORM.equals(glyph)) {
            double min = lower != null ? lower : (params.size() > 0 ? params.get(0) : 0.0);
            double max = upper != null ? upper : (params.size() > 1 ? params.get(1) : 1.0);
            return new UniformScalarModel(min, max);
        }
        
        if (GLYPH_BETA.equals(glyph)) {
            double alpha = params.size() > 0 ? params.get(0) : 2.0;
            double beta = params.size() > 1 ? params.get(1) : 2.0;
            double min = lower != null ? lower : 0.0;
            double max = upper != null ? upper : 1.0;
            return new BetaScalarModel(alpha, beta, min, max);
        }
        
        if (GLYPH_GAMMA.equals(glyph)) {
            double shape = params.size() > 0 ? params.get(0) : 2.0;
            double scale = params.size() > 1 ? params.get(1) : 1.0;
            // Gamma usually unbounded above, lower bound implies shift?
            // Original formatModelParams: +shift if location != 0
            // Here we don't have shift syntax yet, assume standard gamma
            return new GammaScalarModel(shape, scale);
        }
        
        if (GLYPH_STUDENT_T.equals(glyph)) {
            // (ν=...,μ=...,σ=...) format or ordered (ν, μ, σ)
            // formatModelParams output: (ν=1,μ=0,σ=1)
            // But parseBasic extracts values. If user provides "1,0,1" it works.
            // If user provides "ν=1,μ=0...", parseValue needs to handle "key=val"
            double df = params.size() > 0 ? params.get(0) : 30.0;
            double loc = params.size() > 1 ? params.get(1) : 0.0;
            double scale = params.size() > 2 ? params.get(2) : 1.0;
            return new StudentTScalarModel(df, loc, scale);
        }
        
        // Add other models as needed
        
        throw new IllegalArgumentException("Unsupported model type: " + glyph);
    }
    
    private static double parseValue(String val) {
        val = val.trim();
        // Handle "key=value" format (e.g. ν=30)
        int eqIdx = val.indexOf('=');
        if (eqIdx >= 0) {
            val = val.substring(eqIdx + 1).trim();
        }
        // Handle Unicode minus
        val = val.replace("−", "-");
        return Double.parseDouble(val);
    }
    
    private static String identifyGlyph(String spec) {
        if (spec.startsWith(GLYPH_NORMAL)) return GLYPH_NORMAL;
        if (spec.startsWith(GLYPH_UNIFORM)) return GLYPH_UNIFORM;
        if (spec.startsWith(GLYPH_BETA)) return GLYPH_BETA;
        if (spec.startsWith(GLYPH_GAMMA)) return GLYPH_GAMMA;
        if (spec.startsWith(GLYPH_INV_GAMMA)) return GLYPH_INV_GAMMA;
        if (spec.startsWith(GLYPH_STUDENT_T)) return GLYPH_STUDENT_T;
        if (spec.startsWith(GLYPH_PEARSON_IV)) return GLYPH_PEARSON_IV;
        if (spec.startsWith(GLYPH_BETA_PRIME)) return GLYPH_BETA_PRIME;
        return null;
    }
    
    private static boolean startsWithGlyph(String spec) {
        return identifyGlyph(spec) != null;
    }
    
    private static List<String> splitComponents(String body) {
        List<String> comps = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '[' || c == '(') depth++;
            else if (c == ']' || c == ')') depth--;
            else if (c == ',' && depth == 0) {
                comps.add(body.substring(start, i));
                start = i + 1;
            }
        }
        if (start < body.length()) {
            comps.add(body.substring(start));
        }
        return comps;
    }
}
