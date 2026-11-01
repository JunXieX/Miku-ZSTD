package mikumc.zstd;

import com.mojang.brigadier.CommandDispatcher;
import com.mojang.brigadier.ParseResults;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import com.mojang.brigadier.tree.LiteralCommandNode;
import com.velocitypowered.api.command.BrigadierCommand;
import com.velocitypowered.api.command.CommandSource;
import com.velocitypowered.api.proxy.ConsoleCommandSource;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.api.proxy.ServerConnection;
import com.velocitypowered.api.scheduler.ScheduledTask;
import com.velocitypowered.api.scheduler.Scheduler;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ZstdCommandRoutingTest {
    private CommandDispatcher<CommandSource> dispatcher;
    private LiteralCommandNode<CommandSource> root;
    private int scheduled;
    private int cancelled;
    private ZstdVelocityConfig savedConfig;
    private Field configPath;
    private Path savedConfigPath;

    @BeforeEach
    void open() throws ReflectiveOperationException {
        savedConfig = ZstdVelocityConfig.INSTANCE;
        configPath = ZstdVelocityConfig.class.getDeclaredField("lastConfigPath");
        configPath.setAccessible(true);
        savedConfigPath = (Path) configPath.get(null);
        ZstdBossBarMonitor.stop();

        ScheduledTask task = fixture(ScheduledTask.class, (self, method, args) -> {
            if (method.getName().equals("cancel")) {
                cancelled++;
                return null;
            }
            throw new AssertionError("Unexpected task call: " + method.getName());
        });
        Scheduler.TaskBuilder builder = fixture(Scheduler.TaskBuilder.class, (self, method, args) -> {
            return switch (method.getName()) {
                case "delay", "repeat" -> self;
                case "schedule" -> {
                    scheduled++;
                    yield task;
                }
                default -> throw new AssertionError("Unexpected builder call: " + method.getName());
            };
        });
        Scheduler scheduler = fixture(Scheduler.class, (self, method, args) -> {
            if (method.getName().equals("buildTask")) return builder;
            throw new AssertionError("Unexpected scheduler call: " + method.getName());
        });
        ProxyServer proxy = fixture(ProxyServer.class, (self, method, args) -> {
            return switch (method.getName()) {
                case "getScheduler" -> scheduler;
                case "getPlayerCount" -> 0;
                default -> throw new AssertionError("Unexpected proxy call: " + method.getName());
            };
        });
        ZstdBossBarMonitor.init(proxy, new Object());
        root = new ZstdCommand(proxy, null).build().getNode();
        dispatcher = new CommandDispatcher<>();
        dispatcher.getRoot().addChild(root);
    }

    @AfterEach
    void close() throws IllegalAccessException {
        try {
            ZstdBossBarMonitor.stop();
        } finally {
            ZstdBossBarMonitor.init(null, null);
            ZstdVelocityConfig.INSTANCE = savedConfig;
            configPath.set(null, savedConfigPath);
        }
    }

    @Test
    void barIsVisibleCompletableAndParsableWithoutProxyPermission() throws Exception {
        SourceFixture player = new SourceFixture(true, true);
        assertTrue(root.canUse(player.source));
        assertTrue(root.getChild("bar").canUse(player.source));
        var suggestions = dispatcher.getCompletionSuggestions(
                dispatcher.parse("mikuzstd ", player.source)).join().getList();
        assertTrue(suggestions.stream().anyMatch(s -> s.getText().equals("bar")));
        List<String> visible = List.of(dispatcher.getAllUsage(root, player.source, true));
        assertTrue(visible.contains("bar"));
        assertFalse(visible.contains("reload"));

        ParseResults<CommandSource> parsed = dispatcher.parse("mikuzstd bar", player.source);
        assertTrue(parsed.getExceptions().isEmpty());
        assertFalse(parsed.getReader().canRead());
        assertEquals(BrigadierCommand.FORWARD, dispatcher.execute(parsed));
        assertEquals(1, player.backendLookups);
        assertTrue(player.messages.isEmpty());
        assertEquals(0, scheduled);
    }

    @Test
    void forwardingPlayerDoesNotStopAnAuthorizedProxyBar() throws Exception {
        SourceFixture owner = new SourceFixture(true, false, "mikuzstd.command");
        assertEquals(1, dispatcher.execute("mikuzstd bar", owner.source));
        assertEquals(1, owner.shown);
        assertEquals(1, scheduled);

        SourceFixture backendPlayer = new SourceFixture(true, true);
        assertEquals(BrigadierCommand.FORWARD, dispatcher.execute("mikuzstd bar", backendPlayer.source));
        assertEquals(0, owner.hidden);
        assertEquals(0, cancelled);
        assertEquals(0, backendPlayer.shown);
        assertTrue(backendPlayer.messages.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"mikuzstd.command", "zstd.command"})
    void proxyPermissionKeepsLocalToggleWithoutNeedingABackend(String permission) throws Exception {
        SourceFixture player = new SourceFixture(true, false, permission);
        assertEquals(1, dispatcher.execute("mikuzstd bar", player.source));
        assertEquals(1, player.shown);
        assertEquals(1, scheduled);
        assertEquals(0, player.backendLookups);
        assertEquals(1, dispatcher.execute("mikuzstd bar", player.source));
        assertEquals(1, player.hidden);
        assertEquals(1, cancelled);
        assertEquals(0, player.backendLookups);
        assertTrue(root.getChild("reload").canUse(player.source));
    }

    @Test
    void playerWithoutPermissionOrBackendGetsAnExplicitDenial() throws Exception {
        SourceFixture player = new SourceFixture(true, false);
        assertEquals(0, dispatcher.execute("mikuzstd bar", player.source));
        assertEquals(1, player.backendLookups);
        assertEquals(1, player.messages.size());
        assertEquals(NamedTextColor.RED, player.messages.getFirst().color());
        assertEquals(0, player.shown);
        assertEquals(0, scheduled);
    }

    @Test
    void consoleWithoutProxyPermissionIsDeniedInsteadOfForwarded() throws Exception {
        SourceFixture console = new SourceFixture(false, false);
        assertInstanceOf(ConsoleCommandSource.class, console.source);
        assertEquals(0, dispatcher.execute("mikuzstd bar", console.source));
        assertEquals(1, console.messages.size());
        assertEquals(NamedTextColor.RED, console.messages.getFirst().color());
        assertEquals(0, scheduled);
    }

    @Test
    void authorizedConsoleRetainsExistingLocalStopBehavior() throws Exception {
        SourceFixture owner = new SourceFixture(true, false, "mikuzstd.command");
        assertEquals(1, dispatcher.execute("mikuzstd bar", owner.source));
        SourceFixture console = new SourceFixture(false, false, "zstd.command");
        assertEquals(1, dispatcher.execute("mikuzstd bar", console.source));
        assertEquals(1, owner.hidden);
        assertEquals(1, cancelled);
        assertEquals(1, console.messages.size());
    }

    @Test
    void reloadStaysPermissionGatedWhileStatusAndTopStayPublic() throws Exception {
        SourceFixture player = new SourceFixture(true, true);
        assertFalse(root.getChild("reload").canUse(player.source));
        assertThrows(CommandSyntaxException.class,
                () -> dispatcher.execute("mikuzstd reload", player.source));
        assertTrue(root.getChild("status").canUse(player.source));
        assertTrue(root.getChild("top").canUse(player.source));
        assertEquals(1, dispatcher.execute("mikuzstd", player.source));
        assertEquals(1, dispatcher.execute("mikuzstd status", player.source));
        assertEquals(1, dispatcher.execute("mikuzstd top", player.source));
        assertFalse(player.messages.isEmpty());
        assertEquals(0, scheduled);
    }

    // 只夹具外部接口；路由、权限判断和监控切换均调用真实生产实现。
    private static final class SourceFixture implements InvocationHandler {
        private final Set<String> permissions;
        private final ServerConnection backend;
        private final CommandSource source;
        private final List<Component> messages = new ArrayList<>();
        private int backendLookups;
        private int shown;
        private int hidden;

        private SourceFixture(boolean player, boolean connected, String... permissions) {
            this.permissions = Set.of(permissions);
            backend = connected ? fixture(ServerConnection.class, (self, method, args) -> {
                throw new AssertionError("Unexpected backend call: " + method.getName());
            }) : null;
            source = player ? fixture(Player.class, this) : fixture(ConsoleCommandSource.class, this);
        }

        @Override
        public Object invoke(Object self, Method method, Object[] args) {
            return switch (method.getName()) {
                case "hasPermission" -> permissions.contains(args[0]);
                case "getCurrentServer" -> {
                    backendLookups++;
                    yield Optional.ofNullable(backend);
                }
                case "isActive" -> true;
                case "sendMessage" -> {
                    messages.add((Component) args[0]);
                    yield null;
                }
                case "showBossBar" -> {
                    shown++;
                    yield null;
                }
                case "hideBossBar" -> {
                    hidden++;
                    yield null;
                }
                default -> throw new AssertionError("Unexpected source call: " + method.getName());
            };
        }
    }

    private static <T> T fixture(Class<T> type, InvocationHandler handler) {
        Object proxy = Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (self, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "equals" -> self == args[0];
                            case "hashCode" -> System.identityHashCode(self);
                            case "toString" -> type.getSimpleName() + "Fixture";
                            default -> throw new AssertionError(method.getName());
                        };
                    }
                    return handler.invoke(self, method, args);
                });
        return type.cast(proxy);
    }
}
