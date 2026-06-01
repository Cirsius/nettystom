![banner](banner_dark.png#gh-dark-mode-only)
![banner](banner_light.png#gh-light-mode-only)

# Minestom

[![license](https://img.shields.io/github/license/Minestom/Minestom?style=for-the-badge&color=b2204c)](../LICENSE)
[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=for-the-badge)](https://github.com/RichardLitt/standard-readme)  
[![javadocs](https://img.shields.io/badge/documentation-javadocs-4d7a97?style=for-the-badge)](https://javadoc.minestom.net)
[![wiki](https://img.shields.io/badge/documentation-wiki-74aad6?style=for-the-badge)](https://wiki.minestom.net/)
[![discord-banner](https://img.shields.io/discord/706185253441634317?label=discord&style=for-the-badge&color=7289da)](https://discord.gg/pkFRvqB)

Minestom is an open-source library that enables developers to create their own Minecraft server software, without any code from Mojang.

The main difference between Mojang's vanilla server and a Minestom-based server is that ours does not contain any features by default!
Instead, we provide a complete, modern API designed to let you build anything you want, with ease.

> [!IMPORTANT]
> This is a developer API, not a drop-in server meant to be used by end-users. Replacing Bukkit/Forge/Sponge with Minestom **will not work**, since we do not implement any of their APIs. You write your server in Java (or another JVM language) on top of the Minestom library.

Minestom currently targets **Minecraft 1.21.11** and requires **Java 25 or newer**.

# Table of contents
- [Install](#install)
- [Usage](#usage)
- [Quick start](#quick-start)
- [Why Minestom?](#why-minestom)
- [Advantages & Disadvantages](#advantages-and-disadvantages)
- [API](#api)
- [Credits](#credits)
- [Contributing](#contributing)
- [License](#license)

# Install
Minestom is not installed like Bukkit/Forge/Sponge.
As Minestom is a Java library, it must be loaded the same way any other Java library may be loaded.
This means you need to add Minestom as a dependency, add your code and compile by yourself.

Minestom is available on [Maven Central](https://mvnrepository.com/artifact/net.minestom/minestom),
and can be installed like the following (Gradle/Kotlin):

[![](https://img.shields.io/maven-central/v/net.minestom/minestom)](https://mvnrepository.com/artifact/net.minestom/minestom)

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("net.minestom:minestom:<latest release>")
    
    // If you want to use the integration testing library.
    testImplementation("net.minestom:testing:<latest release>")
}
```

PR branches tagged with the "Publish Pull Request" tag are published to the maven central snapshot repository, which can
be used to test new features before they are released. The version for these snapshots is `<branch>-SNAPSHOT`, where 
`<branch>` is the name of the branch.

```kotlin
repositories {
    maven(url = "https://central.sonatype.com/repository/maven-snapshots/") {
        content { // This filtering is optional, but recommended
            includeModule("net.minestom", "minestom")
            includeModule("net.minestom", "testing")
        }
    }
    mavenCentral()
}

dependencies {
    implementation("net.minestom:minestom:<branch>-SNAPSHOT")
    testImplementation("net.minestom:testing:<branch>-SNAPSHOT")
}
```

<details>
<summary>Pinning snapshot versions</summary>

By default, `<branch>-SNAPSHOT` versions will always resolve to the latest snapshot version, meaning the dependency
can update without you changing anything in your build file (and possibly be inconsistent between people if gradle
has cached an older version, by default for 24h).

To pin the snapshot version to a specific release you can reference the exact build. There are two places to find this:
* The maven-metadata.xml, combine the parts of `snapshot.timestamp` and `snapshot.buildNumber`. For example, the 1.21.6
  branch is currently published as `1_21_6-SNAPSHOT` and `1_21_6-20250707.141325-4`.
* In the "External Libraries" section of IntelliJ, if you expand the `-SNAPSHOT` jar it will show the pinnable 
  version which you can use.

</details>

# Usage
A full example of how to use the Minestom library is available [here](/demo).
Alternatively you can check the official [wiki](https://wiki.minestom.net/) or the [javadocs](https://javadoc.minestom.net).

# Quick start
The snippet below boots a minimal server: it creates an instance, generates a simple flat world, and spawns players on it. Run it on Java 25+ and connect with a Minecraft 1.21.11 client on `localhost:25565`.

```java
public class Server {
    public static void main(String[] args) {
        // Initialize the server
        MinecraftServer minecraftServer = MinecraftServer.init();

        // Create an in-memory instance (a "world") and fill the floor with blocks
        InstanceManager instanceManager = MinecraftServer.getInstanceManager();
        InstanceContainer instance = instanceManager.createInstanceContainer();
        instance.setGenerator(unit ->
                unit.modifier().fillHeight(0, 40, Block.GRASS_BLOCK));

        // Spawn every player on the instance at a fixed position
        GlobalEventHandler eventHandler = MinecraftServer.getGlobalEventHandler();
        eventHandler.addListener(AsyncPlayerConfigurationEvent.class, event -> {
            event.setSpawningInstance(instance);
            event.getPlayer().setRespawnPoint(new Pos(0, 42, 0));
        });

        // Start listening for connections
        minecraftServer.start("0.0.0.0", 25565);
    }
}
```

> [!NOTE]
> The server above runs in offline mode. To enable Mojang authentication call `MojangAuth.init()` before starting the server.

# Why Minestom?
Minecraft has evolved a lot since its release, most of the servers today do not take advantage of vanilla features and even have to struggle because of them.
Our target audience is those who want to make a server that benefits little from vanilla features. e.g. creative, kitpvp.
The goal is to offer more performance for those who need it.
In other words, it makes sense to use Minestom when it takes less time to implement every missing vanilla feature you want than removing every vanilla feature that will slow you down.

# Advantages and Disadvantages
Minestom isn't perfect, our choices make it much better for some cases, worse for some others.

## Advantages
* Remove the overhead of vanilla features
* Multi-threaded
* Instance system (Collections of blocks and entities) which is much more scalable than worlds
* Open-source
* Modern API
* No more legacy NMS

## Disadvantages
* Does not work with Bukkit/Forge/Sponge plugins or mods
* Does not work with older clients (using a proxy with ViaBackwards is possible)
* Bad for those who want a vanilla experience
* Longer to develop something playable
* Multi-threaded environments need extra consideration

# API
Even if we do not include anything by default in the game, we simplify the way you add them, here is a preview.

## Instances
It is our major concept, worlds are great for survival with friends, but when it scales up it can become unmanageable. The best examples can be found in Skyblock or minigames, not being able to separate each part properly and being forced to save everything in files, not to say the overhead caused by unnecessary data contained in them. Instances are a lightweight solution to it, being able to have every chunk in memory only, copying and sending it to another player in no time, with custom serialization and much more...

Being able to create instances directly on the go is a must-have, we believe it can push many more projects forward.

Instances also come with performance benefits, unlike some others which will be fully single-threaded or maybe using one thread per world we are using a set number of threads (pool) to manage all chunks independently from instances, meaning using more CPU power.

## Blocks
By default, Minestom does not know what a chest is; you have to tell it that the block should open an inventory.
Every "special" block (one that isn't purely visual) needs a specialized handler. Once that handler is applied, you have a block that can be placed anywhere.
All blocks are still visually there, they simply won't have any interaction by default.

## Entities
The terms "passive" or "aggressive" monsters do not exist; nothing stops you from making a flying chicken rush at any player coming too close. Doing the same with NMS is a real mess because of obfuscation and the deep inheritance hierarchy.

## Inventories
It is a field where Minecraft evolved a lot, inventories are now used a lot as client<->server interface with clickable items and callback, we support these interactions natively without the need of programming your solution.

## Commands
Commands are the simplest way of communication between clients and server. Since 1.13 Minecraft has incorporated a new library denominated "Brigadier", we then integrated an API designed to use the full potential of args types.

# Credits
* The [contributors](https://github.com/Minestom/Minestom/graphs/contributors) of the project
* [The Minecraft Coalition](https://minecraft.wiki/w/Minecraft_Wiki:Projects/wiki.vg_merge) and [`#mcdevs`](https://github.com/mcdevs) -
   protocol and file formats research.
* [The Minecraft Wiki](https://minecraft.wiki) for all their useful info
* [JProfiler](https://www.ej-technologies.com/products/jprofiler/overview.html) for their amazing Java profiler

# Contributing
See [the contributing file](CONTRIBUTING.md)!
All WIP features are previewed as Draft PRs

# License
This project is licensed under the [Apache License Version 2.0](../LICENSE).
