#!/usr/bin/env python3
"""Génère la carte de partage social (og:image) : static/img/og-card.png.

RÔLE
    Produit l'image 1200x630 référencée par les balises og:image / twitter:image
    dans website/templates/_base.html. C'est l'aperçu affiché quand une page du
    site est partagée sur les réseaux sociaux, dans un chat ou cité par un LLM.

QUAND LA RÉGÉNÉRER
    Script ponctuel, PAS appelé au runtime ni au build : le PNG produit est
    commité dans le dépôt et servi tel quel comme asset statique. Relancer ce
    script uniquement quand le visuel doit changer (logo, texte, couleurs) puis
    commiter le nouveau static/img/og-card.png.

FONCTIONNEMENT
    1. Rasterise le logo SVG du projet (static/img/logo.svg) en bitmap via resvg.
    2. Compose sur un fond blanc 1200x630 : logo à gauche, texte mono à droite.
    3. Écrit le PNG optimisé dans static/img/og-card.png.

POURQUOI resvg (et pas cairosvg)
    Le logo contient des <mask> (les trous blancs des pins) et un
    mix-blend-mode:multiply (recouvrement des deux pins). cairosvg rasterise ces
    couches à la résolution du viewBox (16px) puis les upscale -> trous flous et
    blend ignoré. resvg (moteur Rust) les rasterise à la résolution device :
    trous nets et recouvrement multiply correct, fidèle au rendu navigateur.

DÉPENDANCES / LANCEMENT
    Pillow (déjà dans le projet) + resvg-py. resvg-py n'est qu'un outil de
    génération, pas une dépendance runtime : on l'injecte à la volée via uv pour
    ne pas polluer pyproject.toml.

        uv run --with resvg-py python scripts/gen_og_card.py
"""

import io
import os.path

import resvg_py
from PIL import Image, ImageDraw, ImageFont

# Chemins : ROOT = racine du dépôt (ce script vit dans scripts/).
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGO_SVG = os.path.join(ROOT, "static", "img", "logo.svg")
OUT_PNG = os.path.join(ROOT, "static", "img", "og-card.png")

# Dimensions imposées par le standard Open Graph (ratio 1.91:1).
W, H = 1200, 630
WHITE = (255, 255, 255)   # fond de la carte
DARK = (24, 24, 33)       # texte principal
GRAY = (110, 110, 125)    # texte secondaire (non utilisé pour l'instant)

# Polices monospace (cohérence avec la typo font-mono du site).
MONO_B = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf"
MONO_R = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"


def main() -> None:
    # 1. Rasterisation du logo. svg_to_bytes renvoie un PNG (liste d'octets) ;
    #    on le relit en mémoire avec Pillow. viewBox du SVG = carré 16x16, donc
    #    largeur = hauteur pour garder le ratio des pins.
    logo_h = logo_w = 300
    png_bytes = resvg_py.svg_to_bytes(svg_path=LOGO_SVG, width=logo_w, height=logo_h)
    logo = Image.open(io.BytesIO(bytes(png_bytes))).convert("RGBA")

    # 2. Composition. Canevas blanc, puis collage du logo à gauche, centré
    #    verticalement. Le 3e argument (logo) sert de masque alpha -> garde la
    #    transparence autour des pins.
    img = Image.new("RGB", (W, H), WHITE)
    img.paste(logo, (90, (H - logo_h) // 2), logo)
    d = ImageDraw.Draw(img)

    # 3. Bloc texte à droite du logo (x = marge gauche + largeur logo + gouttière).
    #    Coordonnées y ajustées pour centrer le bloc sur la hauteur du logo.
    x = 90 + logo_w + 70
    d.text((x, 212), "atp2osm", font=ImageFont.truetype(MONO_B, 92), fill=DARK)
    d.text((x, 330), "Enrichir OpenStreetMap", font=ImageFont.truetype(MONO_B, 40), fill=DARK)
    d.text((x, 378), "avec AllThePlaces", font=ImageFont.truetype(MONO_B, 40), fill=DARK)

    # 4. Écriture du PNG final (optimize = recompression sans perte plus agressive).
    img.save(OUT_PNG, "PNG", optimize=True)
    print(f"written {OUT_PNG} {img.size}")


if __name__ == "__main__":
    main()
