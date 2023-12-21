# typed: false
# frozen_string_literal: true

# This file was generated by GoReleaser. DO NOT EDIT.
class DatabrewBlink < Formula
  desc "Open Source stream processing framework"
  homepage "https://github.com/usedatabrew/blink"
  version "1.4.2"

  on_macos do
    url "https://github.com/usedatabrew/blink/releases/download/v1.4.2/homebrew-databrewblink_1.4.2_darwin_amd64.tar.gz"
    sha256 "5bcbc4904cd94a434cda419c5f2eb58fe285c3306ce26f84f1f8e347b3869782"

    def install
      bin.install "databrew-blink"
    end

    if Hardware::CPU.arm?
      def caveats
        <<~EOS
          The darwin_arm64 architecture is not supported for the DatabrewBlink
          formula at this time. The darwin_amd64 binary may work in compatibility
          mode, but it might not be fully supported.
        EOS
      end
    end
  end

  on_linux do
    if Hardware::CPU.intel?
      url "https://github.com/usedatabrew/blink/releases/download/v1.4.2/homebrew-databrewblink_1.4.2_linux_amd64.tar.gz"
      sha256 "72f87a557805ae8abf5d2b8c9dde2eecc1d494007606b130be5fcfa9dbb83d5e"

      def install
        bin.install "databrew-blink"
      end
    end
  end
end
