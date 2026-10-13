package de.augmentia.quad.worker.tool;

import de.augmentia.quad.core.annotation.Tool;
import de.augmentia.quad.core.tool.ReflectiveToolMethod;
import de.augmentia.quad.core.tool.ToolArgsMapper;
import de.augmentia.quad.core.tool.ToolMethod;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Global immutable catalog of all available built-in tools.
 * Built once at startup and not modified afterwards.
 */
public class ToolCatalog {

    private final Map<String, ToolMethod> catalog;

    public ToolCatalog(ToolArgsMapper argsMapper, Set<Object> builtinToolBeans) {
        Map<String, ToolMethod> tools = new HashMap<>();

        for (Object bean : builtinToolBeans) {
            Class<?> clazz = bean.getClass();
            for (Method method : clazz.getDeclaredMethods()) {
                if (!method.isAnnotationPresent(Tool.class)) {
                    continue;
                }
                // Verhindere Duplikate - erster gewinnt
                tools.putIfAbsent(method.getName(), new ReflectiveToolMethod(method, bean, argsMapper));
            }
        }

        this.catalog = Map.copyOf(tools);
    }

    /**
     * Alle registrierten Tool-Namen.
     */
    public Set<String> names() {
        return catalog.keySet();
    }

    /**
     * Returns a tool by name. Fail-fast on an unknown name.
     */
    public ToolMethod require(String name) {
        ToolMethod tool = catalog.get(name);
        if (tool == null) {
            throw new IllegalArgumentException("Unknown tool: " + name + ". Available: " + catalog.keySet());
        }
        return tool;
    }

    /**
     * Reads a tool without an exception (null if unknown).
     */
    public ToolMethod tryGet(String name) {
        return catalog.get(name);
    }

    /**
     * Whether the tool exists in the catalog.
     */
    public boolean contains(String name) {
        return catalog.containsKey(name);
    }
}