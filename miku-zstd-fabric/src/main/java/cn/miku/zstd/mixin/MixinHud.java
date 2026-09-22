package cn.miku.zstd.mixin;

import net.minecraft.client.DeltaTracker;
import net.minecraft.client.gui.GuiGraphicsExtractor;
import net.minecraft.client.gui.Hud;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import cn.miku.zstd.ZstdHudRenderer;

/**
 * 26.3 将 HUD 渲染重构为 extract 架构（旧 GuiGraphics 已移除），
 * 在 Hud.extractRenderState 尾部追加 zstd 统计 HUD。
 */
@Mixin(Hud.class)
public class MixinHud {

    @Inject(method = "extractRenderState", at = @At("TAIL"))
    private void zstd$onExtractRenderState(GuiGraphicsExtractor extractor, DeltaTracker deltaTracker, CallbackInfo ci) {
        ZstdHudRenderer.render(extractor, deltaTracker);
    }
}
